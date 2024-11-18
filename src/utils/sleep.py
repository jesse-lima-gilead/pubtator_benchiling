import subprocess
import time

# Duration in seconds (8 hours = 8 * 60 * 60 = 28800 seconds)
duration = 8 * 60 * 60

print(f"Keeping your Mac awake for 8 hours...")

try:
    # Run caffeinate command to prevent sleep for the specified duration
    subprocess.run(["caffeinate", "-i", "-d", "-t", str(duration)])
except KeyboardInterrupt:
    print("\nScript interrupted. Your Mac may now sleep normally.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    print("Script finished. Your Mac may now sleep normally.")

# Wait for a moment to show the final message
time.sleep(2)
