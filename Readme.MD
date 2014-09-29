Handy little script I slapped together to copy a folder of choice to another folder.  
  
What makes it different?  
It doesn't care about destination folder organization.  If the file is already in the target folder somewhere, don't copy it again.  
It lets me organize my external storage after I save say... a Downloads folder usb thumbdrive or whatever.  
  
How does it work?  
MD5sum both directory trees.  
Walk that nested list and copy files that are already replicated.  
Preserve directory structure from source if the file/files don't already exist.  
Don't make superfluous copies if the file/folder already exists.  

