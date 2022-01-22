# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'homelandrealestate_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en' 
    # 1. FOLLOWING
    def start_requests(self):

        start_urls = [
            {
                "type" : "House",
                "property_type" : "house"
            },
            {
                "type" : "Apartment",
                "property_type" : "apartment"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = url.get("type")
            data = {
                "action": "all_locations",
                "search": f"house_type={r_type}",
                "lang": "en",
                "api": "6bf4f3a70575bc5db5d1466abd582edc",
                "path": "/en-gb/woningaanbod",
                "center_map": "false",
                "html_lang": "en",
            }

            yield FormRequest(
                url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
                callback=self.parse,
                formdata=data,
                dont_filter=True,
                meta={'property_type': url.get('property_type')})

    def parse(self, response):
        data = json.loads(response.body)
        for key,item in data.items():
            if key.isdigit():
                item_loader = ListingLoader(response=response)
                house_id = item["house_id"]
                city = item["city"]
                street = item["street"]
                zipcode = item["zipcode"]
                external_link = f"https://www.homelandrealestate.nl/en-gb/woning?{city}/{street}/{house_id}".replace(" ","-")

                item_loader.add_value("external_source", "Homelandrealestate_PySpider_" + self.country + "_" + self.locale)
                item_loader.add_value("title", street)
                item_loader.add_value("external_link", external_link)
                item_loader.add_value("property_type", response.meta.get("property_type"))
                item_loader.add_value("external_id",house_id)
                if item['bedrooms']:
                    item_loader.add_value("room_count",item["bedrooms"])
                else:
                    room = response.xpath("//span[contains(.,'Slaapkamer')]/following-sibling::span/text()").get()
                    if room:
                        item_loader.add_value("room_count", room)
                item_loader.add_value("bathroom_count",item["bathrooms"])
                item_loader.add_value("square_meters", item["surface"])
                item_loader.add_value("city",city )
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address",  "{}, {}, {}".format(city,street,zipcode))
                item_loader.add_value("latitude", item["lat"])
                item_loader.add_value("longitude", item["lng"])
                try:
                    rent_value = item["set_price"].split(",")[0].replace(".","")
                    if int(rent_value) > 300000:
                        return
                except:
                    pass
                item_loader.add_value("rent", item["set_price"])
                item_loader.add_value("currency", "EUR")
                furnished = item["interior"]
                if furnished and furnished == "Gemeubileerd":
                    item_loader.add_value("furnished", True)
                status = item["front_status"]
                if status and "Leased" in status:
                    continue
                date_parsed = dateparser.parse(item["available_at"], date_formats=["%d %B %Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                
                item_loader.add_value("landlord_phone", "070-8200920")
                item_loader.add_value("landlord_email", "info@homelandmail.nl")
                item_loader.add_value("landlord_name", "Homeland Real Estate")
                # yield item_loader.load_item()
           
                data = {
                    "action": "property",
                    "property_part": "photo",
                    "photo_version": "2",
                    "url": external_link,
                    "path": "/en-gb/woning",
                    "html_lang": "en",
                }

                yield FormRequest(
                    url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
                    callback=self.get_image,
                    formdata=data,
                    dont_filter=True,
                    meta={
                        "item_loader" : item_loader,
                        "external_link": external_link
                    })
            

    def get_image(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        
        images = [x["middle"] for x in data["photo"]]
        if images:
            item_loader.add_value("images", images)

        # yield item_loader.load_item()
        data = {
                    "action": "property",
                    "property_part": "description",
                    "url": response.meta.get("external_link"),
                    "path": "/en-gb/woning",
                    "html_lang": "en",
                }

        yield FormRequest(
            url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.get_description,
            formdata=data,
            dont_filter=True,
            meta={
                "item_loader" : item_loader 
            }
        )            

    def get_description(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        
        item_loader.add_value("description", data["description"])

        yield item_loader.load_item()
        