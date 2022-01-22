# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy.spiders import SitemapSpider
from w3lib.html import remove_tags
from python_spiders.items import ListingItem
from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re  

class MySpider(SitemapSpider):
    name = "goimmobilier_be"
    # sitemap_urls = ["https://www.goimmobilier.be/sitemap.xml"]
    sitemap_rules = [
        ("/a-louer/", "parse"), 
    ] 
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    
    def start_requests(self):
        url = "https://www.goimmobilier.be/page-data/fr/a-louer/page-data.json"
        yield Request(url, self.parse)
    
    def parse(self, response): 

        data = json.loads(response.body)
        
        json_data = data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]
        for item in json_data:
            if item["language"] == "fr":
                type = item["TypeDescription"].lower()
                property_type = ""
                property = ""
                if "maison" in type or "huis" in type or "duplex" in type or "villa" in type or "house" in type:
                    property_type = "house"
                elif "appart" in type:
                    property_type = "apartment"
                elif "studio" in type:
                    property_type = "studio"
                elif "kamer" in type or "kot" in type:
                    property_type = "room"
                else:
                    continue
                
                city = item["City"].replace(" ","-").replace("-","").replace("Ê","E").replace("É","E").lower()
                type = type.lower().strip().replace(" - ","--").replace("à","a").replace("é","e").replace("è","e").replace(" ","-").replace("Ê","E").replace("!","").replace("+","")
                id = item["ID"]
                ext_url = f"/fr/a-louer/{city}/{type}/{id}/"
                url = f"https://www.goimmobilier.be/page-data{ext_url}page-data.json"
                
                yield Request(url, callback=self.populate_item,
                meta = {"property_type": property_type,"link":ext_url})

        
 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        url = f"https://www.goimmobilier.be{response.meta.get('link')}"
        item_loader.add_value("external_link", url)
        external_link=url 
        if external_link=='https://www.goimmobilier.be/fr/a-louer/': 
            return 
        item_loader.add_value("external_source", "Goimmobilier_PySpider_" + self.country + "_" + self.locale)
        external_id=url.split("-")[-1].strip() 
        external_idd=re.findall("\d+",external_id) 
        item_loader.add_value("external_id", external_idd) 
 
        value = json.loads(response.body)["result"]["pageContext"]["metadata"]["fr"]
        item_loader.add_value("title", str(value["pageTitle"]).replace("goimmo",""))
        item_loader.add_value("description", str(value["metadataDescription"]).replace("goimmo",""))

 
        if "schema" in value: 
            schema = json.loads(response.body)["result"]["pageContext"]["metadata"]["fr"]["schema"]
            for s in schema:
                if "numberOfRooms" in s:
                    item_loader.add_value("room_count", s["numberOfBedrooms"])
                    item_loader.add_value("square_meters", s["floorSize"])
                    item_loader.add_value("property_type", s["@type"].lower())

                if "priceSpecification" in s:
                    item_loader.add_value("rent", s["priceSpecification"]["price"])
                # if  not "priceSpecification" in s: 
                #     break  
                if "location" in s:
                    item_loader.add_value("zipcode", s["location"]["postalCode"])
                    item_loader.add_value("city", s["location"]["addressLocality"])
                    item_loader.add_value("address", "{} {}".format(s["location"]["postalCode"],s["location"]["addressLocality"]))

        item_loader.add_value("currency", "EUR")
        value_property = json.loads(response.body)["result"]["pageContext"]["data"]["contentRow"] 
        for item in value_property: 
            if "property" in item: 
                item_loader.add_value("longitude", item["property"]["GoogleY"])
                item_loader.add_value("latitude", item["property"]["GoogleX"])
                item_loader.add_value("images", item["property"]["LargePictures"])
                item_loader.add_value("landlord_phone", item["property"]["ManagerDirectPhone"])
                item_loader.add_value("landlord_email",  item["property"]["ManagerEmail"])
                item_loader.add_value("landlord_name",  item["property"]["ManagerName"])
                garage=item["property"]["NumberOfGarages"]
                if garage:
                    item_loader.add_value("parking",True)
                bathroom=item["property"]["NumberOfBathRooms"]
                if bathroom:
                    item_loader.add_value("bathroom_count",bathroom)
                bathroomcheck=item_loader.get_output_value("bathroom_count")
                if not bathroomcheck:
                    item_loader.add_value("bathroom_count",item["property"]["NumberOfShowerRooms"])
                date=item["property"]["DateFree"]
                if date:
                    date=str(date)
                    item_loader.add_value("available_date",date.split(" ")[0])
        
        if not item_loader.get_collected_values("landlord_name"):
            item_loader.add_value("landlord_name", "Go Immobilier")
        if not item_loader.get_collected_values("landlord_phone"):
            item_loader.add_value("landlord_phone", "0479/20.19.19")
        if not item_loader.get_collected_values("landlord_email"):
            item_loader.add_value("landlord_email", "info@goimmobilier.be")
        
        yield item_loader.load_item()
                    
              
        
        # prop = json.loads(response.body)["pageContext"]["data"]["contentRow"][0]
        # if "property" not in prop.keys():
        #     return
        
        # prop = prop["property"]

        # prop_type = prop.get("WebIDName")
        # property_type = ""
        # if "appartement" in prop_type.lower():
        #     property_type = "apartment"
        # elif "maison" in prop_type.lower():
        #     property_type = "house"
        # elif "studio" in prop_type.lower():
        #     property_type = "studio"
        # elif "duplex" in prop_type.lower():
        #     property_type = "apartment"
        # elif "villa" in prop_type.lower():
        #     property_type = "house"
        # elif "immeuble" in prop_type.lower():
        #     property_type = "house"
        # elif "villa/maison/ferme" in prop_type.lower():
        #     property_type = "house"
        # elif "local commercial " in  prop_type.lower():
        #     pass

        # if property_type: 
            

            
        #     item_loader.add_value("external_link", ext_url)
        #     item_loader.add_value("zipcode", prop.get("Zip"))

        #     title = prop.get("TypeDescription")
        #     if "Studio" in title:
        #         property_type = "studio"
        #         item_loader.add_value("property_type","studio")
        #     else:
        #         item_loader.add_value("property_type",property_type )

        #     item_loader.add_value("title",title )
        #     energy = str(prop.get("EnergyPerformance"))
        #     if "None" not in energy:
        #         item_loader.add_value("energy_label",energy)

        #     utilities = prop.get("ChargesRenter")
        #     if utilities !=0:
        #         item_loader.add_value("utilities",utilities )
        #     else:
        #         desc = prop.get("DescriptionA")
        #         if "euros/mois" in desc:
        #             number = re.findall(r'\d+(?:\.\d+)?', desc.split("euros/mois")[0])
        #             utilities = number[-1]
        #             item_loader.add_value("utilities", utilities)
        #         elif "€/mois" in desc:
        #             numbers = re.findall(r'\d+(?:\.\d+)?', desc.split("mois")[0])
        #             utilities =numbers[-1]
        #             item_loader.add_value("utilities", utilities)

        #     item_loader.add_value("city", prop.get("City"))

        #     item_loader.add_value("description", prop.get("DescriptionA"))
        #     price = str(prop.get("Price"))
        #     if price:
        #         item_loader.add_value("rent", price)
                
        #     item_loader.add_value("external_id", str(prop.get("ID")))

        #     square = prop.get("SurfaceTotal")
        #     room = prop.get("NumberOfBedRooms")
            
        #     if square :
        #         if square !=0:
        #             item_loader.add_value("square_meters", str(square))

        #     item_loader.add_value(
        #         "available_date", str(prop.get("DateFree")).split(" ")[0]
        #     )
            
        #     if "NumberOfBedRooms" in prop:
        #         if "studio" in item_loader.get_collected_values("property_type") and room == 0:
        #             item_loader.add_value("room_count", "1")
        #         else:
        #             if room !=0:
        #                 item_loader.add_value("room_count", str(room))

        #         item_loader.add_value("longitude", str(prop.get("GoogleY")))
        #         item_loader.add_value("latitude", str(prop.get("GoogleX")))
        #         bath = prop.get("NumberOfBathRooms")
        #         if bath != 0:
        #             item_loader.add_value("bathroom_count", bath )
        #         else:
        #             bath = prop.get("NumberOfShowerRooms")
        #             if bath != 0:
        #                 item_loader.add_value("bathroom_count", bath )
        #         terrace = prop.get("NumberOfGarages")
        #         if terrace:
        #             if terrace == 1:
        #                 item_loader.add_value("parking", True)
        #             elif terrace == 0:
        #                 item_loader.add_value("parking", False)

        #         item_loader.add_value("elevator", prop.get("HasLift"))
        #         images = [response.urljoin(x) for x in prop.get("SmallPictures")]
        #         if images:
        #             item_loader.add_value("images", images)
        #         item_loader.add_value("landlord_name", prop.get("ManagerName"))
        #         item_loader.add_value("landlord_email", prop.get("ManagerEmail"))
        #         phone = prop.get("ManagerMobilePhone")
        #         # if phone:
        #         # elif not phone:
                    
        #         item_loader.add_value("address", (prop.get("Zip") + " " + prop.get("City")))

            
                # yield item_loader.load_item()

