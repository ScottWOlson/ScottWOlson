![showcase](https://user-images.githubusercontent.com/23444983/186924328-d6439361-5fb3-45bd-a4ee-77715e588b3f.png)

## What iz thiz?!

It's a utility to perform analytics on csv data. Specifically, NYC [Buildings](https://data.cityofnewyork.us/Housing-Development/Buildings-Subject-to-HPD-Jurisdiction/kj4p-ruqc/data) and [Registration](https://data.cityofnewyork.us/Housing-Development/Registration-Contacts/feu5-w2e2/data) datasets. [`pandas`](https://pandas.pydata.org/) is excellent for moderate workloads like these and as such has been integrated into the backend. To make it easier for end-user to upload and export data directly from the browser, a lightweight frontend is provided using the [Flask](https://flask.palletsprojects.com/en/2.2.x/) framework. All the IO as been abstracted away with a basic rpc endpoint at `/process`, allowing you - as a stellar developer - to simply add a form with input `name="fuction"`, `value="python-function-name"` and watch the magic unfold ✨🧝✨

### Adding a new feature walkthrough

#### Suppose you wish to count distinct column values in an uploaded csv

1. Add the backend rpc function under `api/process.py`
   ```python
   @register
   def count_distinct_values():
     file = request.files.get('exotic-csv')
     download_name = request.form.get('download_name')
     df = pd.read_csv(file)
     df = num_distinct_column_values(df)
     return export(df, download_name)
   ```
2. Add the corresponding form in `templates/main.html`
   ```html
   <form action="/process" method="post" enctype="multipart/form-data">
     <div class="header">Count distinct column values</div>
     <input type="hidden" name="function" value="count_distinct_values" />
     {{ forms.file(name='exotic-csv', label='Choose a file...') }}
     <input type="text" name="download_name" value="counting-foreva" />
     <input type="submit" value="Process" />
   </form>
   ```

3. Run the server using `python main.py` and head over to http://localhost:8000/ to blast away your exotic csv! 🚀 🥙

## Security
Try to avoid saving and reading files from server storage. As of now, the primary hosting environment of this project is public on replit - making it an easy target for exploit - especially when we're dealing with the excel format. If you absolutely must do server-side file IO, thoroughly sanitize both the local and remote input to your rpc function.

## Dataset direct links
[All-Buildings-Subject-to-HPD-Jurisdiction](https://data.cityofnewyork.us/api/views/kj4p-ruqc/rows.csv?accessType=DOWNLOAD)

[All-Registration-Contacts](https://data.cityofnewyork.us/api/views/feu5-w2e2/rows.csv?accessType=DOWNLOAD)

[Registration-Contacts-For-COOP-or-CONDO](https://data.cityofnewyork.us/api/id/feu5-w2e2.csv?$query=select%20*%20where%20(upper(%60contactdescription%60)%20=%20upper('CO-OP')%20or%20upper(%60contactdescription%60)%20=%20upper('CONDO'))%20limit%201000000)  
*Note that column names are in small caps*
