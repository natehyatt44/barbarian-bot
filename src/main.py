import discord
import random
from discord import Message as DiscordMessage
import logging
from src.base import Message, Conversation
from src.constants import (
    BOT_INVITE_URL,
    TENOR_KEY,
    DISCORD_BOT_TOKEN,
    EXAMPLE_CONVOS,
    ACTIVATE_THREAD_PREFX,
    MAX_THREAD_MESSAGES,
    SECONDS_DELAY_RECEIVING_MSG,
)
import asyncio
from src.utils import (
    logger,
    should_block,
    close_thread,
    is_last_message_stale,
    discord_message_to_message,
)
from src import completion
from src.completion import generate_completion_response, process_response
from src.moderation import (
    moderate_message,
    send_moderation_blocked_message,
    send_moderation_flagged_message,
)
import requests
import datetime
import pytz
import boto3
import csv
import uuid
from io import StringIO
import re

# set the timezone to Mountain Standard Time (MST)
mst = pytz.timezone('US/Mountain')
# get the current datetime in MST
now = datetime.datetime.now(mst)

logging.basicConfig(
    format="[%(asctime)s] [%(filename)s:%(lineno)d] %(message)s", level=logging.INFO
)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

client = discord.Client(intents=intents)
tree = discord.app_commands.CommandTree(client)

@client.event
async def on_ready():
    logger.info(f"We have logged in as {client.user}. Invite URL: {BOT_INVITE_URL}")
    completion.MY_BOT_NAME = client.user.name
    completion.MY_BOT_EXAMPLE_CONVOS = []
    for c in EXAMPLE_CONVOS:
        messages = []
        for m in c.messages:
            if m.user == "B-TeamChairMan":
                messages.append(Message(user=client.user.name, text=m.text))
            else:
                messages.append(m)
        completion.MY_BOT_EXAMPLE_CONVOS.append(Conversation(messages=messages))
    await tree.sync()

    # Add this line to start the check_inactivity function as a background task
    #client.loop.create_task(check_inactivity())

# /chat message:
@tree.command(name="chat", description="Create a new thread for conversation")
@discord.app_commands.checks.has_permissions(send_messages=True)
@discord.app_commands.checks.has_permissions(view_channel=True)
@discord.app_commands.checks.bot_has_permissions(send_messages=True)
@discord.app_commands.checks.bot_has_permissions(view_channel=True)
@discord.app_commands.checks.bot_has_permissions(manage_threads=True)
async def chat_command(int: discord.Interaction, message: str):
    try:
        # only support creating thread in text channel
        if not isinstance(int.channel, discord.TextChannel):
            return

        # block servers not in allow list
        if should_block(guild=int.guild):
            return

        user = int.user
        logger.info(f"Chat command by {user} {message[:20]}")
        try:
            # moderate the message
            flagged_str, blocked_str = moderate_message(message=message, user=user)
            await send_moderation_blocked_message(
                guild=int.guild,
                user=user,
                blocked_str=blocked_str,
                message=message,
            )
            if len(blocked_str) > 0:
                # message was blocked
                await int.response.send_message(
                    f"Your prompt has been blocked by moderation.\n{message}",
                    ephemeral=True,
                )
                return

            embed = discord.Embed(
                description=f"<@{user.id}> wants to chat! 🤖💬",
                color=discord.Color.green(),
            )
            embed.add_field(name=user.name, value=message)

            if len(flagged_str) > 0:
                # message was flagged
                embed.color = discord.Color.yellow()
                embed.title = "⚠️ This prompt was flagged by moderation."

            await int.response.send_message(embed=embed)
            response = await int.original_response()

            await send_moderation_flagged_message(
                guild=int.guild,
                user=user,
                flagged_str=flagged_str,
                message=message,
                url=response.jump_url,
            )
        except Exception as e:
            logger.exception(e)
            await int.response.send_message(
                f"Failed to start chat {str(e)}", ephemeral=True
            )
            return

        # create the thread
        thread = await response.create_thread(
            name=f"{ACTIVATE_THREAD_PREFX} {user.name[:20]} - {message[:30]}",
            slowmode_delay=1,
            reason="gpt-bot",
            auto_archive_duration=60,
        )
        async with thread.typing():
            # fetch completion
            messages = [Message(user=user.name, text=message)]
            response_data = await generate_completion_response(
                messages=messages, user=user
            )
            # send the result
            await process_response(
                user=user, channel=thread, response_data=response_data
            )
    except Exception as e:
        logger.exception(e)
        await int.response.send_message(
            f"Failed to start chat {str(e)}", ephemeral=True
        )


# calls for each message
@client.event
async def on_message(message: DiscordMessage):
    try:
        channel = message.channel
        # block servers not in allow list
        if should_block(guild=message.guild):
            return

        # ignore messages from the bot
        if message.author == client.user:
            return

        # checks for gif requests
        if message.content.startswith('!gif'):
            searchTerm = message.content[5:]
            if len(searchTerm) == 0:
                searchTerm = 'WWF Wrestling'
            gif_url = await get_gif(searchTerm)
            await message.channel.send(gif_url)

        # checks for good mornings
        if message.content.startswith('!gm') or message.content.startswith('!story') or message.content.startswith('!chat'):
            channel_messages = [
                discord_message_to_message(message)
            ]
            channel_messages = [x for x in channel_messages if x is not None]
            channel_messages.reverse()

            async with channel.typing():
                response_data = await generate_completion_response(
                    messages=channel_messages, user=message.author
                )

            if is_last_message_stale(
                    interaction_message=message,
                    last_message=channel.last_message,
                    bot_id=client.user.id,
            ):
                # there is another message and its not from us, so ignore this response
                return

            # send response
            await process_response(
                user=message.author, channel=channel, response_data=response_data
            )

        # checks for recaps, looks at the last 50 messages and says something about them
        if message.content.startswith('!recap'):
            channel_messages = [
                discord_message_to_message(message)
                async for message in channel.history(limit=50)
            ]
            channel_messages = [x for x in channel_messages if x is not None]
            channel_messages.reverse()

            async with channel.typing():
                response_data = await generate_completion_response(
                    messages=channel_messages, user=message.author
                )

            if is_last_message_stale(
                    interaction_message=message,
                    last_message=channel.last_message,
                    bot_id=client.user.id,
            ):
                # there is another message and its not from us, so ignore this response
                return

            # send response
            await process_response(
                user=message.author, channel=channel, response_data=response_data
            )

        # ignore other messages not in a thread
        if not isinstance(channel, discord.Thread):
            return

        # ignore threads not created by the bot
        thread = channel
        if thread.owner_id != client.user.id:
            return

        # ignore threads that are archived locked or title is not what we want
        if (
            thread.archived
            or thread.locked
            or not thread.name.startswith(ACTIVATE_THREAD_PREFX)
        ):
            # ignore this thread
            return

        if thread.message_count > MAX_THREAD_MESSAGES:
            # too many messages, no longer going to reply
            await close_thread(thread=thread)
            return

        # moderate the message
        flagged_str, blocked_str = moderate_message(
            message=message.content, user=message.author
        )
        await send_moderation_blocked_message(
            guild=message.guild,
            user=message.author,
            blocked_str=blocked_str,
            message=message.content,
        )
        if len(blocked_str) > 0:
            try:
                await message.delete()
                await thread.send(
                    embed=discord.Embed(
                        description=f"❌ **{message.author}'s message has been deleted by moderation.**",
                        color=discord.Color.red(),
                    )
                )
                return
            except Exception as e:
                await thread.send(
                    embed=discord.Embed(
                        description=f"❌ **{message.author}'s message has been blocked by moderation but could not be deleted. Missing Manage Messages permission in this Channel.**",
                        color=discord.Color.red(),
                    )
                )
                return
        await send_moderation_flagged_message(
            guild=message.guild,
            user=message.author,
            flagged_str=flagged_str,
            message=message.content,
            url=message.jump_url,
        )
        if len(flagged_str) > 0:
            await thread.send(
                embed=discord.Embed(
                    description=f"⚠️ **{message.author}'s message has been flagged by moderation.**",
                    color=discord.Color.yellow(),
                )
            )

        # wait a bit in case user has more messages
        if SECONDS_DELAY_RECEIVING_MSG > 0:
            await asyncio.sleep(SECONDS_DELAY_RECEIVING_MSG)
            if is_last_message_stale(
                interaction_message=message,
                last_message=thread.last_message,
                bot_id=client.user.id,
            ):
                # there is another message, so ignore this one
                return

        logger.info(
            f"Thread message to process - {message.author}: {message.content[:50]} - {thread.name} {thread.jump_url}"
        )

        channel_messages = [
            discord_message_to_message(message)
            async for message in thread.history(limit=MAX_THREAD_MESSAGES)
        ]
        channel_messages = [x for x in channel_messages if x is not None]
        channel_messages.reverse()

        # generate the response
        async with thread.typing():
            response_data = await generate_completion_response(
                messages=channel_messages, user=message.author
            )

        if is_last_message_stale(
            interaction_message=message,
            last_message=thread.last_message,
            bot_id=client.user.id,
        ):
            # there is another message and its not from us, so ignore this response
            return

        # send response
        await process_response(
            user=message.author, channel=thread, response_data=response_data
        )
    except Exception as e:
        logger.exception(e)

@client.event
async def on_member_join(member: discord.Member):
    channel = discord.utils.get(member.guild.channels, name="✨°general")
    await channel.send(f"Looks like {member.mention} has joined the server!")
    join_message = [
        Message(user=member.mention, text=f"""Hey B-TeamChairMan its me {member.mention}, 
                             I am new here can I get big welcome greeting from BarbarianInc? """)
        ]

    async with channel.typing():
        response_data = await generate_completion_response(
            messages=join_message, user=member.mention
        )
    # send response
    await process_response(
        user=member.mention, channel=channel, response_data=response_data
    )

async def get_gif(searchTerm):
    print("https://tenor.googleapis.com/v2/search?q={}&key={}&limit=50".format(searchTerm, TENOR_KEY))
    response = requests.get("https://tenor.googleapis.com/v2/search?q={}&key={}&limit=50".format(searchTerm, TENOR_KEY))
    data = response.json()
    gifs = data['results']

    if len(gifs) == 0:
        return None

    random_gif = random.choice(gifs)
    gif_url = random_gif['media_formats']['gif']['url']

    return gif_url

# async def check_inactivity():
#     inactivity_threshold = 6 * 60 * 60  # 6 hours in seconds
#     gif_threshold = 10 * 60 * 60  # 10 hours in seconds
#     target_guild_id = 1053818243732754513
#
#     while True:
#         await asyncio.sleep(60)  # Check every minute
#
#         target_guild = client.get_guild(target_guild_id)
#         target_channel = discord.utils.get(target_guild.channels, name="✨°general")
#         if target_channel is None:
#             continue
#
#         if target_channel.id not in check_inactivity.last_active:
#             check_inactivity.last_active[target_channel.id] = datetime.datetime.now(mst)
#
#         if target_channel.id not in check_inactivity.last_gif:
#             check_inactivity.last_gif[target_channel.id] = datetime.datetime.now(mst) - datetime.timedelta(seconds=gif_threshold)
#
#         time_since_last_active = datetime.datetime.now(mst) - check_inactivity.last_active[target_channel.id]
#         time_since_last_gif = datetime.datetime.now(mst) - check_inactivity.last_gif[target_channel.id]
#
#         if time_since_last_active.total_seconds() >= inactivity_threshold:
#             if time_since_last_gif.total_seconds() >= gif_threshold:
#                 check_inactivity.last_gif[target_channel.id] = datetime.datetime.now(mst)
#                 gif_url = await get_gif("hello chat")
#                 if gif_url is not None:
#                     await target_channel.send(gif_url)
#
# # Initialize the last_active and last_gif dictionaries as attributes of the check_inactivity function
# check_inactivity.last_active = {}
# check_inactivity.last_gif = {}

import re


@tree.command(name="accountcode", description="Create or Retrieve your Lost Ones Account Code")
async def accountCode(ctx, account_id: str):
    allowed_channel_id = 1105203074039042179  # Replace with the actual channel ID
    if ctx.channel_id == allowed_channel_id:
        await process_account_code(ctx, account_id)
    else:
        await ctx.response.send_message("This command can only be used in the '🧩°the-lost-ones' channel")


async def process_account_code(ctx, account_id: str):
    if not re.match(r"0\.0\.\d{5,}", account_id):
        await ctx.response.send_message(
            "Invalid Account ID format. The account ID must be numbers and follow this format: '0.0.xxxxxx'")
        return

    s3 = boto3.client('s3')
    bucket_name = 'lost-ones-upload32737-staging'
    object_key = f'public/accountCode/accountCodes.csv'

    try:
        # Download the CSV from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Read CSV content
        csv_reader = csv.reader(StringIO(csv_content), delimiter='|')
        account_codes = {row[0]: row[1] for row in csv_reader}

        # Generate a UUID if the account_id does not exist
        if account_id not in account_codes:
            account_codes[account_id] = str(uuid.uuid4())
            note = "Created"

            # Save the updated CSV back to S3
            csv_out = StringIO()
            csv_writer = csv.writer(csv_out, delimiter='|')
            for account_id, account_code in account_codes.items():
                csv_writer.writerow([account_id, account_code])

            s3.put_object(
                Bucket=bucket_name, Key=object_key, Body=csv_out.getvalue()
            )
        else:
            note = "Retrieved"

        # Create an embed for the accountCode response
        embed = discord.Embed(
            title=f"{note} {account_id} Account Code",
            description=f"{account_codes[account_id]}",
            color=discord.Color.blue()
        )

        # Send the accountCode as an embed
        await ctx.response.send_message(embed=embed)

    except Exception as e:
        await ctx.response.send_message(f"An error occurred while processing the account code: {str(e)}")


client.run(DISCORD_BOT_TOKEN)
