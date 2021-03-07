import path from 'path';
import fs from 'fs';

import { Meteor } from 'meteor/meteor';
import { Random } from 'meteor/random';
import { Accounts } from 'meteor/accounts-base';
import fileType from 'file-type';
import emojione from 'emojione';
import moment from 'moment';
import 'moment-timezone';
import _ from 'underscore';

import {
	Base,
	ProgressStep,
	Selection,
	SelectionChannel,
	SelectionUser,
} from '../../importer/server';
import { Users, Rooms, Messages, EmojiCustom } from '../../models';
import { sendMessage, createDirectRoom } from '../../lib';
import { getValidRoomName } from '../../utils';
import { settings } from '../../settings/server';
// import { MentionsParser } from '../../mentions/lib/MentionsParser';
import { FileUpload } from '../../file-upload/server';

export class ZulipImporter extends Base {
	constructor(info, importRecord) {
		super(info, importRecord);
		this.userTags = [];
		this.bots = {};

		this.zlib = require('zlib');
		this.tarStream = require('tar-stream');
		this.extract = this.tarStream.extract();
		this.path = path;

		this.userFilesDir = '/tmp/zulip-user-files';
	}

	_parseData(data) {
		const dataString = data.toString();
		try {
			this.logger.debug('parsing file contents');
			return JSON.parse(dataString);
		} catch (e) {
			console.error(e);
			return false;
		}
	}

	_convertDate(num) {
		return new Date(num * 1000);
	}

	_arrayToDictionary(collection, idField) {
		// Optimize linear search in the array.
		return Object.fromEntries(
			collection.map((x) => [x[idField], x]),
		);
	}

	_addToCollection(json, collectionType, extractData, rocketCollectionType = '', bucket = 0) {
		this.logger.debug(`preparing ${ collectionType }`);

		const tempContainer = [];
		for (const m of json) {
			const newData = extractData(m);
			if (newData === false) {
				continue;
			}

			tempContainer.push(newData);
		}

		const collectionId = {
			import: this.importRecord._id,
			importer: this.name,
			type: collectionType,
			bucket,
		};
		this.collection.update(collectionId, {
			$push: {
				[collectionType]: { $each: tempContainer },
			},
		}, {
			upsert: true,
		});

		if (rocketCollectionType) {
			const counts = this.importRecord.count || {};
			const updateCountType = `count.${ rocketCollectionType }`;
			super.updateRecord({ [updateCountType]: (counts[rocketCollectionType] || 0) + tempContainer.length });
			super.addCountToTotal(tempContainer.length);
			this.logger.debug(`added ${ tempContainer.length } objects`);
		}
	}

	_prepareUsers(realmJson) {
		this._addToCollection(realmJson.zerver_userprofile, 'users', (u) => {
			if (u.email === '') {
				return false;
			}

			const atPos = u.email.indexOf('@');
			const userName = atPos < 0 ? u.email : u.email.substring(0, atPos);
			let uniqUserName = userName;
			while (this.seenUserNames.has(uniqUserName)) {
				uniqUserName = `${ userName }-${ Random.id() }`;
			}
			this.seenUserNames.add(uniqUserName);

			return {
				id: u.id,
				email: u.email,
				userName: uniqUserName,
				recipientId: u.recipient,
				fullName: u.full_name,
				dateJoined: this._convertDate(u.date_joined),
				isActive: u.is_active,
				isBot: u.is_bot,
				botOwnerId: u.bot_owner,
				isAdministrator: u.role === 200,
				defaultLanguage: u.default_language,
				timeZone: u.timezone,
				isUploadedAvatar: u.avatar_source === 'U',
				// Used only at import preparation step.
				selectionId: Random.id(),
			};
		}, 'users');
	}

	_prepareGroups(realmJson) {
		this._addToCollection(realmJson.zerver_huddle, 'groups', (r) => ({
			id: r.id,
			recipientId: r.recipient,
			// Used only at import preparation step.
			selectionId: Random.id(),
		}), 'channels');
	}

	_prepareStreams(realmJson) {
		this._addToCollection(realmJson.zerver_stream, 'streams', (s) => {
			if (s.deactivated) {
				return false;
			}

			return {
				id: s.id,
				recipientId: s.recipient,
				name: s.name,
				dateCreated: this._convertDate(s.date_created),
				description: s.description,
				isPrivate: s.invite_only,
				isHistoryPublic: s.history_public_to_subscribers,
				firstMessageId: s.first_message_id,
				// Used only at import preparation step.
				selectionId: Random.id(),
			};
		}, 'channels');
	}

	_prepareRecipients(realmJson) {
		this._addToCollection(realmJson.zerver_recipient, 'recipients', (r) => {
			const types = { 1: 'direct', 2: 'stream', 3: 'group' };
			return {
				id: r.id,
				type: types[r.type],
				// 'type_id' corresponds to one of the following:
				//   1 - 'id' in the zerver_userprofile for direct messages;
				//   2 - 'id' in the zerver_stream for streams.
				//   3 - 'id' in the zerver_huddle for groups;
				typeId: r.type_id,
			};
		});
	}

	_prepareReactions(realmJson) {
		this._addToCollection(realmJson.zerver_reaction, 'reactions', (r) => ({
			id: r.id,
			userId: r.user_profile,
			messageId: r.message,
			emojiCode: r.emoji_code,
			emojiType: r.reaction_type === 'unicode_emoji' ? 'unicode' : 'custom',
		}));
	}

	_prepareSubscriptions(realmJson) {
		this._addToCollection(realmJson.zerver_subscription, 'subscriptions', (s) => ({
			id: s.id,
			userId: s.user_profile,
			recipientId: s.recipient,
			isMuted: s.is_muted,
			isFavorite: s.pin_to_top,
		}));
	}

	_prepareAttachments(json) {
		this._addToCollection(json.zerver_attachment, 'attachments', (a) => ({
			id: a.id,
			fileName: a.file_name,
			path: this.path.join(this.userFilesDir, 'uploads', a.path_id),
			createTime: this._convertDate(a.create_time),
			messageIds: a.messages,
		}));
	}

	_prepareAvatars(json) {
		this._addToCollection(json, 'avatars', (a) => {
			if (!a.path.endsWith('.png')) {
				return false;
			}

			return {
				userId: a.user_profile_id,
				path: this.path.join(this.userFilesDir, 'avatars', a.path),
			};
		});
	}

	_prepareCustomEmojis(json) {
		this._addToCollection(json, 'emojis', (e) => {
			if (e.deactivated) {
				return false;
			}

			let mimeType;
			if (e.path.endsWith('.png')) {
				mimeType = 'image/png';
			} else if (e.path.endsWith('.jpg')) {
				mimeType = 'image/jpeg';
			} else if (e.path.endsWith('.gif')) {
				mimeType = 'image/gif';
			} else {
				return false;
			}

			return {
				name: e.name,
				creatorId: e.author,
				path: this.path.join(this.userFilesDir, 'emoji', e.path),
				fileName: e.file_name,
				mimeType,
			};
		});
	}

	_prepareCustomEmojiIds(realmJson) {
		this._addToCollection(realmJson.zerver_realmemoji, 'emoji_ids', (e) => ({
			id: e.id,
			fileName: e.file_name,
		}));
	}

	_prepareMessages(json, bucket) {
		this._addToCollection(json.zerver_message, 'messages', (m) => ({
			id: m.id,
			senderId: m.sender,
			recipientId: m.recipient,
			subject: m.subject,
			content: m.content,
			dateSent: this._convertDate(m.date_sent),
			editHistory: m.edit_history,
			hasAttachment: m.has_attachment,
			hasImage: m.has_image,
			hasLink: m.has_link,
			bucket,
		}), 'messages', bucket);
	}

	_prepareFile(data, zulipFileName) {
		const userFilesSelectors = [
			{ dir: 'avatars/', except: 'avatars/records.json' },
			{ dir: 'emoji/', except: 'emoji/records.json' },
			{ dir: 'realm_icons/', except: undefined },
			{ dir: 'uploads/', except: undefined },
		];
		if (userFilesSelectors.some((s) => zulipFileName.startsWith(s.dir) && zulipFileName !== s.except)) {
			const fsFilePath = this.path.join(this.userFilesDir, zulipFileName);
			fs.mkdirSync(this.path.dirname(fsFilePath), { recursive: true });
			fs.writeFileSync(fsFilePath, data, { encoding: 'binary' });
			return;
		}

		if (!zulipFileName.endsWith('.json')) {
			return;
		}

		const json = this._parseData(data);
		if (json === false) {
			this.logger.error('failed to parse data');
			return false;
		}

		this.seenUserNames = new Set();

		switch (zulipFileName) {
			case 'realm.json':
				super.updateProgress(ProgressStep.PREPARING_USERS);
				this._prepareUsers(json);
				this._prepareReactions(json);
				this._prepareSubscriptions(json);
				this._prepareCustomEmojiIds(json);

				super.updateProgress(ProgressStep.PREPARING_CHANNELS);
				this._prepareStreams(json);
				this._prepareGroups(json);
				this._prepareRecipients(json);

				this.collection.insert({
					import: this.importRecord._id,
					importer: this.name,
					type: 'info',
					info: {
						zulipBaseURL: `https://${ json.zerver_realm[0].name }`,
						description: json.zerver_realm.description,
						dateCreated: json.zerver_realm.date_created,
					},
				});
				break;
			case 'attachment.json':
				this._prepareAttachments(json);
				break;
			case 'avatars/records.json':
				this._prepareAvatars(json);
				break;
			case 'emoji/records.json':
				this._prepareCustomEmojis(json);
				break;
			default:
				const match = zulipFileName.match(/messages-([0-9]+)\.json/);
				if (match) {
					if (!this.switchedToPreparingMessages) {
						this.switchedToPreparingMessages = true;
						super.updateProgress(ProgressStep.PREPARING_MESSAGES);
					}

					// Messages start with 1, hence we offset the bucket to 0.
					this._prepareMessages(json, parseInt(match[1]) - 1);
				} else {
					this.logger.error(`Zulip importer doesn't know what to do with the file "${ zulipFileName }"`);
				}
				break;
		}

		return 0;
	}

	_fetchCollection(name, bucket = 0) {
		return this.collection.findOne({
			import: this.importRecord._id,
			importer: this.name,
			type: name,
			bucket,
		});
	}

	_fetchMessages() {
		if (!this.allMessages) {
			const arrayOfMessages = [];
			for (let bucket = 0, messages = this._fetchCollection('messages', bucket);
				messages; bucket++, messages = this._fetchCollection('messages', bucket)) {
				arrayOfMessages.push(messages.messages);
			}

			this.allMessages = arrayOfMessages.flat();
		}
		return this.allMessages;
	}

	_bindUsersAvatars(users, avatars) {
		for (const user of users) {
			if (!user.isUploadedAvatar) {
				continue;
			}

			const avatar = avatars.find((a) => a.userId === user.id);
			if (avatar) {
				user.avatarPath = avatar.path;
			}
		}
	}

	_bindStreamsCreators(streams, users, messagesDictionary) {
		for (const stream of streams) {
			// It's assumed that the first sender to a stream is the stream's creator.
			const firstMessage = messagesDictionary[stream.firstMessageId];
			if (!firstMessage) {
				continue;
			}

			const sender = users.find((u) => u.id === firstMessage.senderId);
			if (!sender) {
				continue;
			}

			stream.creatorId = sender.id;
		}
	}

	_bindStreamsSubscribers(streams, users, recipients, subscriptions) {
		for (const stream of streams) {
			const streamRecipient = recipients.find((r) => r.id === stream.recipientId);
			if (!streamRecipient || streamRecipient.type !== 'stream') {
				continue;
			}

			const recipientSubscriptions = subscriptions.filter((s) => s.recipientId === streamRecipient.id);
			if (!recipientSubscriptions) {
				continue;
			}

			stream.subscribers = recipientSubscriptions.map((s) => ({
				userId: s.userId,
				userName: (users.find((u) => u.id === s.userId) || {}).userName,
				// Flags below describe the current stream for the given subscriber.
				isMuted: s.isMuted,
				isFavorite: s.isFavorite,
			}));
		}
	}

	_bindGroupsCreators(groups, messagesDictionary) {
		for (const group of groups) {
			// It's assumed that the first sender to a group is the group's creator.
			const firstMessage = messagesDictionary[group.recipientId];
			if (!firstMessage) {
				continue;
			}

			group.creatorId = firstMessage.senderId;
			group.dateCreated = firstMessage.dateSent;
		}
	}

	_bindGroupsSubscribers(groups, users, recipients, subscriptions) {
		for (const group of groups) {
			const groupRecipient = recipients.find((r) => r.id === group.recipientId);
			if (!groupRecipient || groupRecipient.type !== 'group') {
				continue;
			}

			const recipientSubscriptions = subscriptions.filter((s) => s.recipientId === groupRecipient.id);
			if (!recipientSubscriptions) {
				continue;
			}

			group.subscribers = recipientSubscriptions.map((s) => ({
				userId: s.userId,
				userName: (users.find((u) => u.id === s.userId) || {}).userName,
			}));
			group.name = `group[${ group.subscribers.map((s) => s.userName).join(',') }]`;
		}
	}

	_bindCustomEmojiIds(emojis, emojiIds) {
		const emojiIdsDictionary = this._arrayToDictionary(emojiIds, 'fileName');
		for (const emoji of emojis) {
			if (emojiIdsDictionary[emoji.fileName]) {
				emoji.id = emojiIdsDictionary[emoji.fileName].id;
			}
		}
	}

	_enrichMessagesAndUpdateCollections(messagesDictionary) {
		const changedMessages = {};
		const addToChangedMessages = (message) => {
			const bucketArray = changedMessages[message.bucket];
			if (bucketArray) {
				bucketArray.push(message);
			} else {
				changedMessages[message.bucket] = [message];
			}
		};

		const attachments = this._fetchCollection('attachments');
		for (const attachment of attachments.attachments) {
			for (const messageId of attachment.messageIds) {
				const message = messagesDictionary[messageId];
				if (!message || !message.hasAttachment) {
					continue;
				}

				if (!message.attachments) {
					message.attachments = [];
				}

				try {
					message.attachments.push({
						size: fs.statSync(attachment.path).size,
						name: attachment.fileName,
						path: attachment.path,
						date: attachment.createTime,
					});
				} catch (e) {
					this.logger.debug(`failed to access file ${ attachment.path }`);
					this.logger.error(e);
				}

				addToChangedMessages(message);
			}
		}

		const reactions = this._fetchCollection('reactions');
		for (const reaction of reactions.reactions) {
			const message = messagesDictionary[reaction.messageId];
			if (!message) {
				continue;
			}

			if (!message.reactions) {
				message.reactions = [];
			}

			message.reactions.push(reaction);

			addToChangedMessages(message);
		}

		for (const bucket of Object.keys(changedMessages)) {
			const messages = this._fetchCollection('messages', parseInt(bucket));

			for (const changedMessage of changedMessages[bucket]) {
				const message = messages.messages.find((m) => m.id === changedMessage.id);
				Object.assign(message, changedMessage);
			}

			this.collection.update({ _id: messages._id }, { $set: { messages: messages.messages } });
		}
	}

	_finishPreparationProcess(resolve) {
		super.updateProgress(ProgressStep.USER_SELECTION);

		const users = this._fetchCollection('users');
		const avatars = this._fetchCollection('avatars');
		const recipients = this._fetchCollection('recipients');
		const subscriptions = this._fetchCollection('subscriptions');
		const messagesDictionary = this._arrayToDictionary(this._fetchMessages(), 'id');

		this._bindUsersAvatars(users.users, avatars.avatars);

		const streams = this._fetchCollection('streams');

		this._bindStreamsCreators(streams.streams, users.users, messagesDictionary);
		this._bindStreamsSubscribers(streams.streams, users.users, recipients.recipients, subscriptions.subscriptions);

		const groups = this._fetchCollection('groups');

		this._bindGroupsCreators(groups.groups, messagesDictionary);
		this._bindGroupsSubscribers(groups.groups, users.users, recipients.recipients, subscriptions.subscriptions);

		this.collection.update({ _id: users._id }, { $set: { users: users.users } });
		this.collection.update({ _id: streams._id }, { $set: { streams: streams.streams } });
		this.collection.update({ _id: groups._id }, { $set: { groups: groups.groups } });

		const emojis = this._fetchCollection('emojis');
		const emojiIds = this._fetchCollection('emoji_ids');

		this._bindCustomEmojiIds(emojis.emojis, emojiIds.emoji_ids);

		this.collection.update({ _id: emojis._id }, { $set: { emojis: emojis.emojis } });

		this._enrichMessagesAndUpdateCollections(messagesDictionary);

		const selectionUsers = users.users.map(
			(user) => new SelectionUser(user.selectionId, user.fullName, user.email, false, user.isBot, !user.isBot));
		const selectionStreams = streams.streams.map(
			(stream) => new SelectionChannel(stream.selectionId, stream.name, false, true, stream.isPrivate, undefined, false));
		const selectionGroups = groups.groups.map(
			(group) => new SelectionChannel(group.selectionId, group.name, false, true, true));
		const selectionMessages = this.importRecord.count.messages;

		resolve(new Selection(this.name, selectionUsers, selectionStreams.concat(selectionGroups), selectionMessages));
	}

	prepareUsingLocalFile(fullFilePath) {
		this.logger.debug('start preparing import operation');

		this.collection.remove({});
		fs.rmdirSync(this.userFilesDir, { force: true, recursive: true });
		fs.mkdirSync(this.userFilesDir);

		this.switchedToPreparingMessages = false;

		const promise = new Promise((resolve, reject) => {
			this.extract.on('entry', Meteor.bindEnvironment((header, stream, next) => {
				this.logger.debug(`new entry from import file: ${ header.name }`);
				if (header.type !== 'file') {
					stream.resume();
					return next();
				}

				const info = this.path.parse(header.name);
				const slashPos = info.dir.indexOf('/');

				let zulipDirName = '';
				if (slashPos !== -1) {
					zulipDirName = info.dir.substring(slashPos + 1);
				}
				const zulipFileName = this.path.join(zulipDirName, info.base);

				let pos = 0;
				let data = Buffer.allocUnsafe(header.size);
				stream.on('data', Meteor.bindEnvironment((chunk) => {
					data.fill(chunk, pos, pos + chunk.length);
					pos += chunk.length;
				}));
				stream.on('end', Meteor.bindEnvironment(() => {
					this.logger.info(`Processing the file: ${ header.name }`);
					this._prepareFile(data, zulipFileName);
					data = undefined;

					this.logger.debug('next import entry');
					next();
				}));
				stream.on('error', () => next());
				stream.resume();
			}));

			this.extract.on('error', (err) => {
				this.logger.error('extract error:', err);
				reject(new Meteor.Error('error-import-file-extract-error'));
			});

			this.extract.on('finish', Meteor.bindEnvironment(() => {
				this._finishPreparationProcess(resolve, reject);
			}));

			const rs = fs.createReadStream(fullFilePath);
			const gunzip = this.zlib.createGunzip();

			gunzip.on('error', (err) => {
				this.logger.error('extract error:', err);
				reject(new Meteor.Error('error-import-file-extract-error'));
			});
			this.logger.debug('start extracting import file');
			rs.pipe(gunzip).pipe(this.extract);
		});

		return promise;
	}

	_importUsers(startedByUserId, users) {
		for (const user of users) {
			if (!user.do_import) {
				continue;
			}

			this.logger.debug(`importing user ${ JSON.stringify(user) }`);

			Meteor.runAsUser(startedByUserId, () => {
				const existingUser = Users.findOneByEmailAddress(user.email);
				if (existingUser) {
					user.rocketId = existingUser._id;
				} else {
					user.rocketId = Accounts.createUser({
						email: user.email,
						password: '123', // TODO: Date.now() + user.email.toUpperCase(),
					});

					Meteor.runAsUser(user.rocketId, () => {
						Meteor.call('setUsername', user.userName, {
							joinDefaultChannelsSilenced: true,
						});

						const timeZone = moment().tz(user.timeZone);
						if (timeZone) {
							Meteor.call('userSetUtcOffset', parseInt(timeZone.format('Z').toString().split(':')[0]));
						}

						if (user.avatarPath && fs.existsSync(user.avatarPath)) {
							const data = fs.readFileSync(user.avatarPath, { encoding: 'base64' });
							Meteor.call('setAvatarFromService', `data:image/png;base64,${ data }`);
						}
					});

					if (!user.isActive) {
						Meteor.call('setUserActiveStatus', user.rocketId, false);
					}
					if (user.isAdministrator) {
						// TODO: As far as I can see this doesn't work as expected.
						Meteor.call('setAdminStatus', user.rocketId, true);
					}

					Users.setName(user.rocketId, user.fullName);
					Users.setLanguage(user.rocketId, user.defaultLanguage);
					Users.setEmailVerified(user.rocketId, user.email);

					Users.update({ _id: user.rocketId }, { $set: { createdAt: user.dateJoined } });
				}

				Users.update({ _id: user.rocketId }, { $addToSet: { importIds: user.id } });
			});

			super.addCountCompleted(1);
		}
	}

	_importCustomEmojis(startedByUserId, emojis, usersDictionary) {
		for (const emoji of emojis) {
			if (!fs.existsSync(emoji.path)) {
				continue;
			}

			this.logger.debug(`importing custom emoji ${ JSON.stringify(emoji) }`);

			Meteor.runAsUser(startedByUserId, () => {
				const [existingEmoji] = EmojiCustom.findByNameOrAlias(emoji.name).fetch();
				if (existingEmoji) {
					emoji.rocketId = existingEmoji._id;
				} else {
					const creator = usersDictionary[emoji.creatorId] || {};
					Meteor.runAsUser((creator.do_import && creator.rocketId) || startedByUserId, () => {
						const emojiFields = {
							newFile: true,
							name: emoji.name,
							aliases: '',
							extension: this.path.parse(emoji.path).ext.slice(1),
						};
						emoji.rocketId = Meteor.call('insertOrUpdateEmoji', emojiFields);
						const data = fs.readFileSync(emoji.path, { encoding: 'binary' });
						Meteor.call('uploadEmojiCustom', data, emoji.mimeType, emojiFields);
					});

					EmojiCustom.update({ _id: emoji.rocketId }, { $addToSet: { importIds: emoji.id } });
				}
			});
		}
	}

	_importStreams(startedByUserId, streams, usersDictionary) {
		for (const stream of streams) {
			if (!stream.do_import) {
				continue;
			}

			this.logger.debug(`importing stream ${ JSON.stringify(stream) }`);

			Meteor.runAsUser(startedByUserId, () => {
				const existingRoom = Rooms.findOneByNonValidatedName(stream.name);
				if (existingRoom || stream.name.toUpperCase() === 'GENERAL') {
					stream.rocketId = stream.name.toUpperCase() === 'GENERAL' ? 'GENERAL' : existingRoom._id;
				} else {
					const subscribers = [];
					for (const s of stream.subscribers) {
						const u = usersDictionary[s.userId] || {};
						if (u.do_import) {
							subscribers.push(s);
						}
					}
					if (subscribers.length === 0) {
						return;
					}

					const creator = usersDictionary[stream.creatorId] || {};

					Meteor.runAsUser((creator.do_import && creator.rocketId) || startedByUserId, () => {
						stream.rocketId = Meteor.call(
							stream.isPrivate ? 'createPrivateGroup' : 'createChannel',
							getValidRoomName(stream.name), subscribers.map((s) => s.userName),
						)._id;
					});

					if (stream.description) {
						Rooms.setDescriptionById(stream.rocketId, stream.description);
					}

					Meteor.call('archiveRoom', stream.rocketId);

					// TODO: Set stream moderator to one of subscribers addRoomOwner/addRoomModerator.
					// TODO: Add stream to favorites (based on isFavorite).
					// TODO: Disable stream notifications (based on isMuted).

					Rooms.update({ _id: stream.rocketId }, { $set: { ts: stream.dateCreated } });
					Rooms.update({ _id: stream.rocketId }, { $addToSet: { importIds: `stream-${ stream.id }` } });
				}
			});

			super.addCountCompleted(1);
		}
	}

	_importGroups(startedByUserId, groups, usersDictionary) {
		const directRoomMaxUsersCount = settings.get('DirectMesssage_maxUsers') || 1;

		for (const group of groups) {
			if (!group.do_import) {
				continue;
			}

			this.logger.debug(`importing group ${ JSON.stringify(group) }`);

			Meteor.runAsUser(startedByUserId, () => {
				const subscribers = [];
				for (const s of group.subscribers) {
					const u = usersDictionary[s.userId] || {};
					if (u.do_import) {
						subscribers.push(s);
					}
				}
				if (subscribers.length === 0) {
					return;
				}

				const subscribersNames = subscribers.map((s) => s.userName);
				const isDirectRoom = subscribersNames.length <= directRoomMaxUsersCount;
				const existingRoom = Rooms.findDirectRoomContainingAllUsernames(subscribersNames, { fields: { _id: 1 } });
				if (existingRoom) {
					group.rocketId = existingRoom._id;
				} else {
					const creator = usersDictionary[group.creatorId] || {};

					Meteor.runAsUser((creator.do_import && creator.rocketId) || startedByUserId, () => {
						if (isDirectRoom) {
							group.rocketId = createDirectRoom(
								subscribers.map((s) => Users.findOneByImportId(s.userId)))._id;
						} else {
							group.rocketId = Meteor.call('createPrivateGroup', group.name, subscribersNames)._id;
							// TODO: Set group moderator.
						}
					});

					// Direct rooms cannot be archived.
					if (!isDirectRoom) {
						Meteor.call('archiveRoom', group.rocketId);
					}

					Rooms.update({ _id: group.rocketId }, { $set: { ts: group.dateCreated } });
					Rooms.update({ _id: group.rocketId }, { $addToSet: { importIds: `group-${ group.id }` } });
				}
			});

			super.addCountCompleted(1);
		}
	}

	_fixupMessage(text, attachments) {
		return text;
	}

	_uploadAttachments(sender, zulipAttachments, room) {
		const attachments = [];
		for (const zulipAttachment of zulipAttachments) {
			const data = fs.readFileSync(zulipAttachment.path);
			const type = fileType(data);

			const details = {
				name: zulipAttachment.name,
				size: zulipAttachment.size,
				type: type ? type.mime : 'application/octet-stream',
				uploadedAt: zulipAttachment.date,
				userId: sender._id,
				rid: room._id,
			};

			const store = FileUpload.getStore('Uploads');
			const file = store.insertSync(details, data);
			const url = FileUpload.getPath(`${ file._id }/${ encodeURI(file.name) }`);

			const attachment = {
				type: 'file',
				title: file.name,
				title_link: url,
				title_link_download: true,
			};

			if (/^image\/.+/.test(file.type)) {
				attachment.image_url = url;
				attachment.image_type = file.type;
				attachment.image_size = file.size;
				attachment.image_dimensions = file.identify != null ? file.identify.size : undefined;
			}

			if (/^audio\/.+/.test(file.type)) {
				attachment.audio_url = url;
				attachment.audio_type = file.type;
				attachment.audio_size = file.size;
			}

			if (/^video\/.+/.test(file.type)) {
				attachment.video_url = url;
				attachment.video_type = file.type;
				attachment.video_size = file.size;
			}

			attachments.push({ file, attachment });
		}
		return attachments;
	}

	_convertReactions(message) {
		if (!message.reactions) {
			return undefined;
		}

		const reactions = {};
		for (const reaction of message.reactions) {
			let emojiName = '';
			if (reaction.emojiType === 'unicode') {
				const emojiString = String.fromCodePoint(parseInt(reaction.emojiCode, 16));
				emojiName = emojione.toShort(emojiString);
			} else if (reaction.emojiType === 'custom') {
				const emojiCode = parseInt(reaction.emojiCode);
				const customEmoji = EmojiCustom.findOne({ importIds: emojiCode });
				if (!customEmoji) {
					continue;
				}

				emojiName = `:${ customEmoji.name }:`;
			} else {
				this.logger.error('unknown emoji type');
				continue;
			}

			const user = Users.findOneByImportId(reaction.userId);
			if (!user) {
				continue;
			}

			if (!reactions[emojiName]) {
				reactions[emojiName] = { usernames: [] };
			}
			reactions[emojiName].usernames.push(user.username);
		}

		if (_.isEmpty(reactions)) {
			return undefined;
		}
		return reactions;
	}

	_sendMessageImpl(sender, message, room) {
		const reactions = this._convertReactions(message);

		if (message.attachments) {
			const attachments = this._uploadAttachments(sender, message.attachments, room);
			if (attachments.length) {
				Meteor.runAsUser(sender._id, () => {
					const msg = Meteor.call('sendFileMessage', room._id, null, attachments[0].file, {
						// TODO: Remove random.
						_id: `zulip-${ message.id }-attachment-${ Random.id() }`,
						msg: this._fixupMessage(message.content, attachments),
					});

					// Workaround the sendFileMessage restrictions - it doesn't accept ts field
					// (I don't see how to easily add it) and it doesn't accept multiple attachments.
					const update = {
						$set: { ts: message.dateSent, reactions },
					};
					if (attachments.length > 1) {
						update.$addToSet = {
							attachments: {
								$each: attachments.slice(1).map((a) => a.attachment),
							},
						};
					}
					Messages.update({ _id: msg._id }, update);
				});
			}
		} else {
			const details = {
				// TODO: Remove random.
				_id: `zulip-${ message.id }-${ Random.id() }`,
				rid: room._id,
				ts: message.dateSent,
				msg: this._fixupMessage(message.content),
				u: {
					_id: sender._id,
					username: sender.username,
				},
				reactions,
			};

			Meteor.runAsUser(sender._id, () => {
				sendMessage(sender, details, room, true);
			});
		}
	}

	_sendDirectMessage(sender, acceptor, message, roomObjects) {
		const roomId = [acceptor._id, sender._id].sort().join('');
		if (!roomObjects[roomId]) {
			Meteor.runAsUser(sender._id, () => {
				const roomInfo = Meteor.call('createDirectMessage', acceptor.username);
				roomObjects[roomId] = Rooms.findOneById(roomInfo.rid);
			});
		}

		const room = roomObjects[roomId];
		this._sendMessageImpl(sender, message, room);
	}

	_sendStreamMessage(sender, stream, message) {
		this._sendMessageImpl(sender, message, stream);
		// Messages.createRoomSettingsChangedWithTypeRoomIdMessageAndUser('room_changed_topic', room._id, msg.text, creator, { _id: msg.id, ts: msg.ts });
	}

	_importMessages(startedByUserId, messages, recipientsDictionary) {
		const roomObjects = {};
		for (const message of messages) {
			super.updateRecord({ messagesstatus: `${ message.id }/${ messages.length }` });

			Meteor.runAsUser(startedByUserId, () => {
				const sender = Users.findOneByImportId(message.senderId);
				if (!sender) {
					return;
				}

				const recipient = recipientsDictionary[message.recipientId];
				if (!recipient) {
					return;
				}

				if (recipient.type === 'direct') {
					const recipientUser = Users.findOneByImportId(recipient.typeId);
					if (!recipientUser) {
						return;
					}

					this._sendDirectMessage(sender, recipientUser, message, roomObjects);
				} else if (recipient.type === 'stream') {
					const recipientStream = Rooms.findOneByImportId(`stream-${ recipient.typeId }`);
					if (!recipientStream) {
						return;
					}

					this._sendStreamMessage(sender, recipientStream, message);
				} else if (recipient.type === 'group') {
					const recipientGroup = Rooms.findOneByImportId(`group-${ recipient.typeId }`);
					if (!recipientGroup) {
						return;
					}

					this._sendStreamMessage(sender, recipientGroup, message);
				}
			});

			super.addCountCompleted(1);
		}
	}

	startImport(importSelection) {
		this.reloadCount();
		super.startImport(importSelection);

		const start = Date.now();

		const users = this._fetchCollection('users');
		for (const user of importSelection.users) {
			for (const u of users.users) {
				if (u.selectionId === user.user_id) {
					u.do_import = user.do_import;
				}
			}
		}
		this.collection.update({ _id: users._id }, { $set: { users: users.users } });

		const streams = this._fetchCollection('streams');
		for (const stream of importSelection.channels) {
			for (const s of streams.streams) {
				if (s.selectionId === stream.channel_id) {
					s.do_import = stream.do_import;
				}
			}
		}
		this.collection.update({ _id: streams._id }, { $set: { streams: streams.streams } });

		const groups = this._fetchCollection('groups');
		for (const group of importSelection.channels) {
			for (const g of groups.groups) {
				if (g.selectionId === group.channel_id) {
					g.do_import = group.do_import;
				}
			}
		}
		this.collection.update({ _id: groups._id }, { $set: { groups: groups.groups } });

		this.zulipInfo = this.collection.findOne({
			import: this.importRecord._id,
			importer: this.name,
			type: 'info',
		});

		const startedByUserId = Meteor.userId();
		Meteor.defer(() => {
			try {
				super.updateProgress(ProgressStep.IMPORTING_USERS);

				const users = this._fetchCollection('users');
				const emojis = this._fetchCollection('emojis');

				const usersDictionary = this._arrayToDictionary(users.users, 'id');

				this._importUsers(startedByUserId, users.users);
				this.collection.update({ _id: users._id }, { $set: { users: users.users } });
				this._importCustomEmojis(startedByUserId, emojis.emojis, usersDictionary);
				this.collection.update({ _id: emojis._id }, { $set: { emojis: emojis.emojis } });

				super.updateProgress(ProgressStep.IMPORTING_CHANNELS);

				const streams = this._fetchCollection('streams');
				const groups = this._fetchCollection('groups');

				this._importStreams(startedByUserId, streams.streams, usersDictionary);
				this.collection.update({ _id: streams._id }, { $set: { streams: streams.streams } });
				this._importGroups(startedByUserId, groups.groups, usersDictionary);
				this.collection.update({ _id: groups._id }, { $set: { groups: groups.groups } });

				super.updateProgress(ProgressStep.IMPORTING_MESSAGES);

				const messages = this._fetchMessages();
				const recipients = this._fetchCollection('recipients');

				const recipientsDictionary = this._arrayToDictionary(recipients.recipients, 'id');

				this._importMessages(startedByUserId, messages, recipientsDictionary);

				super.updateProgress(ProgressStep.FINISHING);
				super.updateProgress(ProgressStep.DONE);
			} catch (e) {
				this.logger.error(e);
				super.updateProgress(ProgressStep.ERROR);
			}

			fs.rmdirSync(this.userFilesDir, { force: true, recursive: true });

			this.logger.info(`Import took ${ Date.now() - start } milliseconds.`);
		});

		return super.getProgress();
	}
}
