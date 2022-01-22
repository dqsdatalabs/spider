# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
from scrapy.spiders import SitemapSpider
from ..loaders import ListingLoader
from ..helper import *
from scrapy import Request,FormRequest 

class BenjamingaspardSpider(SitemapSpider):
    name = "benjaminGaspard"
    # allowed_domains = ["bg-immo.be"]
    # sitemap_urls = ["https://www.bg-immo.be/sitemap.xml"]
    # sitemap_rules = [
    #     ("/fr/a-louer/", "parse_json"),
    # ]
    start_urls = ["https://www.bg-immo.be/page-data/fr/a-louer/page-data.json"]

    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    def start_requests(self):
        yield Request(self.start_urls[0], callback=self.parse)
      # def parse_json(self, response):
    #     if "maison" in response.url or "appartement" in response.url:
    #         yield response.follow(
    #             response.xpath("//link[@rel='preload'and contains(@href,'json') ]/@href")[0],
    #             self.parse_detail,
    #             cb_kwargs=dict(link=response.url, property_type="house" if "maison" in response.url else "apartment"),
    #         )
    def parse(self, response):
        json_data = json.loads(response.text)
        for item in json_data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
            type = item["TypeDescription"].lower()
            if ("appartement" in type or "maison" in type) and item["language"] == "fr":
                city = item["City"].replace("-","").replace(" ","-").replace("Ê","E").replace("É","").lower()
                
                id = item["ID"]
                ext_url = f"/fr/a-louer/{city}/{type}/{id}/"
                url = f"https://www.bg-immo.be/page-data{ext_url}page-data.json"
                property_type = "house" if "maison" in type else "apartment"
                yield Request(url, callback=self.parse_detail,
                meta = {"property_type": property_type,"link":ext_url})
                

    def parse_detail(self, response):
        """parse detail page """

        property_type = response.meta.get('property_type')
        link = "https://www.bg-immo.be"+response.meta.get('link')

        json_data = json.loads(response.text)
        for item in json_data["result"]["pageContext"]["data"]["contentRow"]:
            item_loader = ListingLoader(response=response)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            item_loader.add_value("external_link", link)
            item = item["property"]
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("room_count", item["NumberOfBedRooms"])
            
            bathroom_count = item["NumberOfBathRooms"]
            if bathroom_count != 0:
                item_loader.add_value("bathroom_count", item["NumberOfBathRooms"])
            else:
                bathroom_count = response.xpath("//div[@class='display-field'][contains(.,'salles')]/following-sibling::div/text()").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.strip())
                    
            item_loader.add_value("city", item["City"])
            item_loader.add_value("zipcode", item["Zip"])
            item_loader.add_value("latitude", item["GoogleX"])
            item_loader.add_value("longitude", item["GoogleY"])
            item_loader.add_value("square_meters", item["SurfaceTotal"])
            item_loader.add_value("currency", "EUR")
            item_loader.add_value("description", item["DescriptionA"])
            item_loader.add_value("title", item["TypeDescription"])
            item_loader.add_value("external_id", str(item["ID"]))
            item_loader.add_value("address", " ".join([str(item["HouseNumber"]), item["Street"]]))
            item_loader.add_value("rent", item["Price"])
            item_loader.add_value("available_date", item["DateFree"].replace("00:00:00", ""))
            item_loader.add_value("images", item["LargePictures"])
            item_loader.add_value("elevator", item["HasLift"])
            item_loader.add_value("parking", item["NumberOfGarages"] > 0)
            item_loader.add_value("landlord_name", item["ManagerName"])
            item_loader.add_value("landlord_phone", item["ManagerMobilePhone"])
            item_loader.add_value("landlord_email", item["ManagerEmail"])
            yield item_loader.load_item()
