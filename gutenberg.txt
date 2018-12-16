# 1. Scrape all 'en' books from a gutenberg mirror into a single directory. Took
#    3h 25m; total size was 11G. Run on Nov 17, 2016. All downloads are ".zip".
wget -m -H -nd "http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en"

# 2. Remove extra crap we get from using wget.
rm robots.txt
rm harvest*

# 3. Remove all duplicate encodings in ISO-<something> and UTF-8. Based on a few
#    random samplings it seemed like there were ASCII versions of all of these.
ls | grep "-8.zip" | xargs rm
ls | grep "-0.zip" | xargs rm

# 4. We were then left with a handful of other files with '-' characters in them
#    (which seemed to be an indication of non-standard formatting).
#     - 89-AnnexI.zip          - trade agreement doc; weird text
#     - 89-Descriptions.zip    - ""
#     - 89-Contents.zip        - ""
#     - 3290-u.zip             - unicode but with "u" instead of "0" suffix
#     - 5192-tex.zip           - tex formatted book
#     - 10681-index.zip        - thesaurus index
#     - 10681-body.zip         - thesaurus body
#     - 13526-page-inames.zip  - (I forget)
#     - 15824-h.zip            - windows-encoded file (I think)
#     - 18251-mac.zip          - mac-encoded file (I think)
#    I removed all of them
rm 89-AnnexI.zip
rm 89-Descriptions.zip
rm 89-Contents.zip
rm 3290-u.zip
rm 5192-tex.zip
rm 10681-index.zip
rm 10681-body.zip
rm 13526-page-inames.zip
rm 15824-h.zip
rm 18251-mac.zip

# 5. unzip all of the files and remove all of the zips.
sudo apt install unzip
unzip "*.zip"
rm "*.zip"

# 6. From foo.zip, some files extract directly into foo.txt, while other extract
#    into foo/foo.txt. Move all into this directory.
mv */*.txt ./

# 7. There will be empty directories left (what we want), and some non-empty
#    ones. The non-empty directories include other formats (e.g. PDF), sneaky
#    nested zips, sneaky nested zips with other formats in them, at least one
#    typo (.TXT), and possibly other things. There are only 20 such directories
#    (including several pdf/mid pairings of the same serial number), so we just
#    remove them along with the empty directories.
ls | grep -v "\.txt" | xargs rm -rf


# The final size of original/ is 37,229 .txt files totaling 14G.
