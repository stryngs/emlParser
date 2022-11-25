
# emlParser
Ever wanted to correlate relationships and other takeaways from a collection of
emails?  emlParser was written for investigatory work around emails.  This tool
should be found useful by researchers who are looking to put the pieces together
of who told what, when, and to whom via an email.

## Concept
Using SQL syntax, visualize the multiple correlations between various senders
and receivers of email; building a set of reference points for analysis.

## Schema layout
- _eml represents the filename
- _time represents when the email was sent or received
- _subj represents the subject of the email
- _from represents the email address of the sender
- _to represents the email address the recipient
- _cc represents the email addresses the email was cc'd to in '\n'.join() format
- _dfrom represents the display name of the sender in '\n'.join() format
- _dto represents the display name of the recipient in '\n'.join() format
- _dcc represents the display name of the cc'd recipients in '\n'.join() format
- _att represents the quantity of detected attachments
- _msg represents the extracted body of the email
- _notes represents a column for the user to modify as needed

## Requirements
```
python3 -m pip install requirements.txt
```

## Directions
1. Put the emails to be parsed into a folder called emails
2. Place the folder called emails in this directory
3. ```python3 ./emlParser.py```

## Upcoming improvements
- Create a table showing individual Cc relationships
- Adding of heatmap visualizations
- Visual tracing of relationships
- Fixing the attachment count bug
