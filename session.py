import os
import sys
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

def fail(msg):
    print(f"[!] {msg}")
    sys.exit(1)

def main():
    load_dotenv()

    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")
    phone = os.getenv("PHONE_NUMBER")
    session_name = os.getenv("SESSION_NAME", "anon")

    if not api_id or not api_hash or not phone:
        fail("Missing API_ID, API_HASH, or PHONE_NUMBER in .env")

    try:
        api_id = int(api_id)
    except ValueError:
        fail("API_ID must be an integer")

    # Use an in-memory StringSession so we can easily print it
    client = TelegramClient(StringSession(), api_id, api_hash)

    async def run():
        await client.connect()

        if not await client.is_user_authorized():
            # Send login code
            try:
                await client.send_code_request(phone)
            except Exception as e:
                fail(f"Failed to send code: {e}")

            code = input("Enter the login code you received: ").strip()
            try:
                await client.sign_in(phone=phone, code=code)
            except SessionPasswordNeededError:
                # 2FA password required
                pwd = input("Two-step verification enabled. Enter your password: ").strip()
                await client.sign_in(password=pwd)
            except Exception as e:
                fail(f"Sign-in failed: {e}")

        # At this point we are authorized
        session_str = client.session.save()
        print("\n=== SESSION STRING ===")
        print(session_str)
        print("======================\n")

        # Optional: also save a local file-based session for convenience
        # (so you can reuse without code entry next time)
        # This does not change the printed StringSession.
        file_client = TelegramClient(session_name, api_id, api_hash)
        await file_client.connect()
        await file_client.import_session(StringSession(session_str))
        await file_client.disconnect()

        await client.disconnect()

    with client:
        client.loop.run_until_complete(run())

if __name__ == "__main__":
    main()
