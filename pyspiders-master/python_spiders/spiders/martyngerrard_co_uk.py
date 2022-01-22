from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

 
class MySpider(Spider):
    name = 'martyngerrard_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Martygerrardcouk_Pyspider_united_kingdom"


    custom_settings = {
       "HTTPCACHE_ENABLED": False,
    }

    headers = {

        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "content-type": "application/x-www-form-urlencoded",
        "Host": "jxn4dtcgy8-dsn.algolia.net",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    }

    start_url = "https://jxn4dtcgy8-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.6.2)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.14.0)&x-algolia-api-key=f66d5f8f7879f856bd74c84c800d47cf&x-algolia-application-id=JXN4DTCGY8"

    form_data = {"requests":[{"indexName":"prod_properties","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&maxValuesPerFacet=10&query=&page=0&facets=%5B%22search_type%22%2C%22publish%22%2C%22department%22%2C%22status%22%2C%22must_include%22%2C%22price%22%2C%22bedroom%22%2C%22building%22%5D&tagFilters=&facetFilters=%5B%5B%22search_type%3Alettings%22%5D%2C%5B%22publish%3Atrue%22%5D%2C%5B%22department%3Aresidential%22%5D%2C%5B%22status%3ATo%20Let%22%2C%22status%3ANew%20Instruction%22%5D%5D"},{"indexName":"prod_properties","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&maxValuesPerFacet=10&query=&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=search_type&facetFilters=%5B%5B%22publish%3Atrue%22%5D%2C%5B%22department%3Aresidential%22%5D%2C%5B%22status%3ATo%20Let%22%2C%22status%3ANew%20Instruction%22%5D%5D"},{"indexName":"prod_properties","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&maxValuesPerFacet=10&query=&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=publish&facetFilters=%5B%5B%22search_type%3Alettings%22%5D%2C%5B%22department%3Aresidential%22%5D%2C%5B%22status%3ATo%20Let%22%2C%22status%3ANew%20Instruction%22%5D%5D"},{"indexName":"prod_properties","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&maxValuesPerFacet=10&query=&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=department&facetFilters=%5B%5B%22search_type%3Alettings%22%5D%2C%5B%22publish%3Atrue%22%5D%2C%5B%22status%3ATo%20Let%22%2C%22status%3ANew%20Instruction%22%5D%5D"},{"indexName":"prod_properties","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&maxValuesPerFacet=10&query=&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=status&facetFilters=%5B%5B%22search_type%3Alettings%22%5D%2C%5B%22publish%3Atrue%22%5D%2C%5B%22department%3Aresidential%22%5D%5D"}]}
    def start_requests(self):

        yield Request(
                        self.start_url,
                        method="POST",
                        callback=self.parse,
                        headers = self.headers,
                        body=json.dumps(self.form_data))  


    def parse(self, response):
        all_data = json.loads((response.body).decode())["results"][0]["hits"]
        for data in all_data:
            item_loader = ListingLoader(response=response)
            if data["status"] and (data["status"] == "Let" or data["status"] == "For Sale" or data["status"] == "Sold"): 
                continue
            
            prop_type = data["building"]
            if get_p_type_string(prop_type):
                item_loader.add_value("property_type",get_p_type_string(prop_type))
            else: 
                continue

            item_loader.add_value("rent",data["price"])
            item_loader.add_value("landlord_name",data["negotiator_details"]["name"])
            item_loader.add_value("landlord_phone",data["negotiator_details"]["telephone"])
            item_loader.add_value("landlord_email",data["negotiator_details"]["email"])

     

            item_loader.add_value("latitude",str(data["_geoloc"]["lat"]))
            item_loader.add_value("longitude",str(data["_geoloc"]["lng"]))

            item_loader.add_value("address",data["display_address"])

            item_loader.add_value("title",data["title"])
            item_loader.add_value("available_date",data["AvailableFrom"])
            item_loader.add_value("zipcode",data["postcode"])
            phonecheck=item_loader.get_output_value("landlord_phone")
            if not phonecheck:
                adrescheck=item_loader.get_output_value("address")
                if "Muswell" in adrescheck:
                    item_loader.add_value("landlord_phone","020 8444 3388")

            
            item_loader.add_value("room_count",data["bedroom"])

            item_loader.add_value("description",data["description"])
            item_loader.add_value("external_id",data["objectID"])

            if data.get("images1"):
                images = []
                images.append(data["images1"][0].get("600x400"))
                if data.get("images2"):
                    for img in data["images2"]:
                        images.append(img["300x200"])
                item_loader.add_value("images",images)
                item_loader.add_value("external_images_count",len(images))
                

            if data.get("floorplan"):
                item_loader.add_value("floor_plan_images",data["floorplan"]["url"])
            
            if data.get("parking"):
                if len(data.get("parking")[0].strip()) > 2:
                    item_loader.add_value("parking",True)

            if "Balcony" in data["situation"]:
                item_loader.add_value("balcony",True)

            item_loader.add_value("city","London")
            item_loader.add_value("currency","GBP")
            item_loader.add_value("external_source",self.external_source)
            external_link = "https://www.martyngerrard.co.uk/property-to-rent/" + data["slug"] + "-" + data["objectID"]
            item_loader.add_value("external_link",external_link)
            id=item_loader.get_output_value("external_id")
            if id:
                url=f"https://martyngerrard-strapi.q.starberry.com/properties/{id}"
                yield Request(url, callback=self.bathroom, meta={"item_loader":item_loader})

    def bathroom(self,response):
        item_loader=response.meta.get("item_loader")
        data = json.loads((response.body).decode())
        bathroom_count=data["bathroom"]
        item_loader.add_value("bathroom_count",bathroom_count)


        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None
        