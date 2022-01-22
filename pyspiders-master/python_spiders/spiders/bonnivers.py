# -*- coding: utf-8 -*-
# Author:
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser
from scrapy import Request,FormRequest
import json
class BonniversSpider(scrapy.Spider):
    name = "bonnivers"
    allowed_domains = ["bonnivers.be"]
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    external_source = 'Bonnivers_PySpider_belgium_fr'
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        start_urls = [
            {"url": "https://www.bonnivers.be/page-data/fr/a-louer/page-data.json", "property_type":"apartment"}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
            )

    def parse(self, response):

        data_json = json.loads(response.body)
        data = data_json["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]
        for item in data:
            property_type=item['MainTypeName']
            
            if property_type:
                
                f_url = item["TypeDescription"].lower().replace("-","").replace(",","").replace(" ","-").replace("à","a").replace("è","e").replace("é","e").replace("l'immobilire","l'immobiliere").replace("è","").replace(":","").replace("/","").strip("-")
                follow_url = f"https://www.bonnivers.be/fr/a-louer/{item['City'].lower()}/{f_url}/{item['ID']}/"
                yield Request(
                    follow_url,
                    callback=self.populate_item,
                    meta={"property_type": property_type, "item":item}
                )
        for item in data:
            property_type=item['MainTypeName']
            
            if property_type:
                
                f_url2 = item["TypeDescription"].lower().replace("-","").replace(",","").replace(" ","-").lower().replace("à","a").replace("è","e").replace("é","e").replace("l'immobilire","l'immobiliere").replace("è","").replace(":","").replace("/","").replace("--","-")
                follow_url2 = f"https://www.bonnivers.be/fr/a-louer/{item['City'].lower()}/{f_url2}/{item['ID']}/"
                yield Request(
                    follow_url2,
                    callback=self.populate_item,
                    meta={"property_type": property_type, "item":item}
                )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        item = response.meta.get('item')
        item_loader.add_value("external_id", str(item['ID']))
        property_type = item['MainTypeName']
        if property_type == 'Maison':
            item_loader.add_value("property_type", "house")
        elif property_type == 'Studio':
            item_loader.add_value("property_type", "studio")
        elif property_type == 'Duplex':
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", "apartment")
        
        title=item["TypeDescription"]
        item_loader.add_value("title",title)
        street = item["Street"]
        city = item["City"]
        zipcode = item["Zip"]
        item_loader.add_value("address", f"{street} {city} {zipcode}")
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        lat = item["GoogleX"]  
        lon = item["GoogleY"]
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lon)
        
        square_meters = item['SurfaceTotal']
        if int(square_meters) > 0:
            item_loader.add_value("square_meters", square_meters)
        elif int(square_meters) == 0:
            square_meters1 = item['SurfaceGround2']
            if square_meters1 > 0:
               item_loader.add_value("square_meters", square_meters1)       
       
        item_loader.add_value("description", item['DescriptionA'])
        
        room_count = item['NumberOfBedRooms']
        if room_count == 0:
            pass
        else:
            item_loader.add_value("room_count", room_count)
        
        bathroomcount=item['NumberOfBathRooms']
        if bathroomcount > 0:
           item_loader.add_value("bathroom_count", bathroomcount)
        elif bathroomcount == 0: 
            bathroomcount1=item['NumberOfShowerRooms']
            if bathroomcount1 > 0:
               item_loader.add_value("bathroom_count", bathroomcount1)
        terrace = item['HasTerrace']
        if terrace:
            item_loader.add_value("terrace", terrace)
        item_loader.add_value("rent", item['Price'])
        item_loader.add_value("currency", "EUR")
        images = []
        images = item['LargePictures']
        if images:
            item_loader.add_value("images", images)

        
        item_loader.add_value("landlord_phone", "081.74.15.51")
        item_loader.add_value("landlord_email", "namur@bonnivers.be")
        item_loader.add_value("landlord_name", "Jacques Bonnivers - Namur")
        yield item_loader.load_item()

