import discord
import random
import re
from discord import Message as DiscordMessage
from discord.ext import tasks
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
from src import getRoles
from src.completion import generate_completion_response, process_response
from src.moderation import (
    moderate_message,
    send_moderation_blocked_message,
    send_moderation_flagged_message,
)
import src.discordNftListing
import src.discordAdminListing
import requests
import datetime
import pytz
import boto3
import csv
import uuid
from io import StringIO
import re
import logging

logging.basicConfig(level=logging.ERROR)

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
    await nft_listings.start()

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

        channel_id = 1068830862617096303

        # checks for admin content
        if message.channel.id == channel_id:
            # checks for admin content
            if message.content.lower().startswith('!cfplist'):
                CFP = '0.0.2235264'
                listings = src.discordAdminListing.execute(CFP)
                top_listings = listings.head(15)
                await message.channel.send("```" + top_listings.to_string() + "```")

            # checks for admin content
            if message.content.lower().startswith('!adlist'):
                AD = '0.0.2371643'
                listings = src.discordAdminListing.execute(AD)
                top_listings = listings.head(15)
                await message.channel.send("```" + top_listings.to_string() + "```")

        # checks for good mornings
        if message.content.lower().startswith('!gm') or 'bteam' in message.content.lower():
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

@tree.command(name="assignrole", description="Input your wallet ID to assign your discord role")
async def assign_role(int: discord.Interaction, account_id: str):
    allowed_channel_id = 1139026502814539990  # Replace with the actual channel ID
    if int.channel_id == allowed_channel_id:
        await process_accounts(int, account_id)
    else:
        await int.response.send_message("This command can only be used in the 🔍°assign-role channel")

async def process_accounts(int: discord.Interaction, account_id: str):
    if not re.match(r"0\.0\.\d{5,}", account_id):
        await int.response.send_message(
            "Invalid Account ID format. The account ID must be numbers and follow this format: '0.0.xxxxxx'")
        return

    s3 = boto3.client('s3')
    bucket_name = 'lost-ones-upload32737-staging'
    object_key = f'public/discordAccounts/accounts.csv'

    try:
        # Try to get the CSV from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
    except s3.exceptions.NoSuchKey:
        # If CSV doesn't exist, set csv_content to an empty string
        csv_content = ""

    discord_username = int.user.name
    discord_user_id = int.user.id
    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # Read CSV content
        csv_reader = csv.reader(StringIO(csv_content), delimiter='|')
        account_data = {row[0]: row for row in csv_reader}

        # Check for existing entries by account_id or discord_username
        account_exists = account_id in account_data
        username_exists = any(data[1] == discord_username for data in account_data.values())

        if not account_exists and not username_exists:
            new_record = [account_id, discord_username, discord_user_id, current_timestamp]
            account_data[account_id] = new_record

            # Save the updated CSV back to S3
            csv_out = StringIO()
            csv_writer = csv.writer(csv_out, delimiter='|')
            for record in account_data.values():
                csv_writer.writerow(record)

            s3.put_object(
                Bucket=bucket_name, Key=object_key, Body=csv_out.getvalue()
            )

        # Fetch NFTs and determine roles (common to both new and existing entries)
        nfts = getRoles.fetch_from_mirror_node(account_id)
        matched_records = getRoles.match_nfts_to_discord_helper(nfts)
        assigned_roles = getRoles.determine_roles(matched_records)

        roles_str = '\n'.join(['- ' + role for role in assigned_roles])


        # Prepare response based on existence check results
        if account_exists:
            title = f"Wallet ID {account_id} already exists ({discord_username})"
            description = f"Roles:\n{roles_str}"
            await assign_roles_to_user(int.user, assigned_roles, int.guild)
        elif username_exists:
            title = f"Discord username {discord_username} already exists for another Wallet ID."
            description = ""
        else:
            title = f"Added Roles for {discord_username}"
            description = f"Roles:\n{roles_str}"
            await assign_roles_to_user(int.user, assigned_roles, int.guild)

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blue()
        )
        await int.response.send_message(embed=embed)

    except Exception as e:
        await int.response.send_message(f"An error occurred while processing the Role: {str(e)}")


async def assign_roles_to_user(member, role_names, guild):
    # List of possible roles to consider for addition/removal
    possible_roles = [
        'Zombie/Spirit',
        'Hbarbarian GOD',
        'Hbarbarian Chieftain',
        'Hbarbarian Berserker',
        'Hbarbarian'
    ]

    # 1. Fetch all the roles that the user currently has.
    current_roles = member.roles

    # Create a list of role objects for the newly determined roles
    new_roles = [discord.utils.get(guild.roles, name=role_name) for role_name in role_names]

    # 2. Identify roles to add and roles to remove based on conditions
    roles_to_add = [role for role in new_roles if role not in current_roles]
    roles_to_remove = [role for role in current_roles if role.name in possible_roles and role.name not in role_names]

    # 3. Add new roles to the user.
    for role in roles_to_add:
        await member.add_roles(role)

    # 4. Remove roles that no longer apply.
    for role in roles_to_remove:
        await member.remove_roles(role)

    # Optional: Print information for debugging.
    for role in roles_to_add:
        print(f"Added role {role.name} to {member.display_name}.")
    for role in roles_to_remove:
        print(f"Removed role {role.name} from {member.display_name}.")


@tree.command(name="refreshroles", description="Refresh Discord roles")
async def refresh_roles(interaction: discord.Interaction):
    allowed_channel_id = 1068830862617096303  # Replace with the actual channel ID
    if interaction.channel_id != allowed_channel_id:
        await interaction.response.send_message("This command can only be used in the dev-progress channel")
        return

    s3 = boto3.client('s3')
    bucket_name = 'lost-ones-upload32737-staging'
    object_key = f'public/discordAccounts/accounts.csv'

    try:
        # Try to get the CSV from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Read CSV content
        csv_reader = csv.reader(StringIO(csv_content), delimiter='|')

        for row in csv_reader:
            account_id, discord_username, discord_user_id, _ = row
            member = discord.utils.get(interaction.guild.members, id=int(discord_user_id))  # get the user from the guild
            print(member)

            # Fetch NFTs and determine roles
            nfts = getRoles.fetch_from_mirror_node(account_id)
            matched_records = getRoles.match_nfts_to_discord_helper(nfts)
            assigned_roles = getRoles.determine_roles(matched_records)

            print (assigned_roles)
            # Assign roles to the user
            await assign_roles_to_user(member, assigned_roles, interaction.guild)

        await interaction.response.send_message("All roles have been refreshed!")

    except Exception as e:
        await interaction.response.send_message(f"An error occurred while refreshing the roles: {str(e)}")

@tasks.loop(minutes=10)
async def nft_listings():
    guild_id = 1053818243732754513  # Replace with your guild id
    channel_id = 1147404638774120448
    guild = discord.utils.get(client.guilds, id=guild_id)
    if not guild:
        print(f"Guild with id {guild_id} not found.")
        return
    channel = discord.utils.get(guild.channels, id=channel_id)
    if not channel:
        print(f"Channel with id {channel_id} not found in guild {guild.name}.")
        return

    CFP = '0.0.2235264'
    AD = '0.0.2371643'
    LO = '0.0.3721853'
    token_ids = [CFP, AD, LO]

    for token_id in token_ids:
        results = src.discordNftListing.execute(token_id)
        if not results:
            print(f"No new listings for token {token_id}.")
            continue
        for result in results:
            embed = discord.Embed(
                title=f"New Listing!\n{result['name']} #{result['serial_number']}",
                color=discord.Color.green()
            )

            embed.set_image(url=result['image_url'])
            if "Bulk" in result['amount']:
                embed.add_field(name="Amount", value=f"[Bulk Listing]({result['market_link']})", inline=True)
            else:
                embed.add_field(name="Amount", value=f"{result['amount']}h", inline=True)
            embed.add_field(name="Seller", value=result['account_id_seller'], inline=True)
            embed.add_field(name="Market", value=f"[{result['market_name']}]({result['market_link']})", inline=True)
            embed.add_field(name="Transaction Time", value=f"{result['txn_time']} UTC", inline=True)
            await channel.send(embed=embed)
            await asyncio.sleep(10)


client.run(DISCORD_BOT_TOKEN)
