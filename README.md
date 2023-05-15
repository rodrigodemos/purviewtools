# Purview Tools

This repo is [a WIP of] a collection of tools that will assist you with common tasks around Microsoft Purview using Python and available SDKs.

Note: Code is provided as-is

## Available Tools

* Bulk download
* Bulk updates
    * Update asset attributes
    * Update asset tags (labels)
    * Update contacts

## Pre-requisites

To be able to run any of the scripts below, you will need:

* Access to Purview
* Python 3.9+

## Prepare your environment

* git clone this repo
* Create venv: python -m venv .venv
* Rename sample.env to .env
* Update .env with your env info
* Activate virtual environment: .venv\scripts\activate
* Install dependencies: pip install -m requirements.txt

## Bulk Download

This script will export into a CSV file the designated assets or glossary terms.

```
py app.py --download --keywords 'sample'
```

## Bulk Update

The update script will look for the following files in the directory set by your env variable UPLOAD_DIRECTORY:
* asset_contacts.csv
* asset_labels.csv
* asset_attributes.csv

```
py app.py --update
```
