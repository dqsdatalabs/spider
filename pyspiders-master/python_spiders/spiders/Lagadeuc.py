# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'Lagadeuc_PySpider_france'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Lagadeuc_PySpider_france'
    # custom_settings = {    
    #     "HTTPCACHE_ENABLED":False,
    #     "CONCURRENT_REQUESTS" : 2,
    #     "AUTOTHROTTLE_ENABLED": True,
    #     "AUTOTHROTTLE_START_DELAY": .5,
    #     "AUTOTHROTTLE_MAX_DELAY": 2,
    #     "RETRY_TIMES": 3,
    #     "DOWNLOAD_DELAY": 1,
    # }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lagadeuc.fr/recherche/?type_trans=2&type_bien2%5B%5D=AP&localisation=&prix_min=0&prix_max=1000000&prix_min2=0&prix_max2=2000",
                ],
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                    dont_filter=True,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        count = response.xpath("count(//div[@class='annonce-list'])").get()
        count = int(count.split(".")[0])
        
        for i in range(count):
            item_loader = ListingLoader(response=response)
            i += 1
            y = 2 * i
            item_loader.add_value("external_source", self.external_source)
            
            follow_url = response.url+f"/{i}"
            item_loader.add_value("external_link", follow_url)
            

            prop_type = response.xpath(f"(//div[@class='infos']//h3/text())[{i}]").get()
            if prop_type and "apartment" in prop_type.lower():
                property_type = "apartment"
            elif prop_type and "maison" in prop_type.lower():
                property_type = "house"
            else:
                property_type = "apartment"

            if property_type:
                item_loader.add_value("property_type", property_type)

            external_id = response.xpath(f"(//div[@class='annonce-list']/@id)[{i}]").get()
            if external_id:
                external_id = external_id.replace("item-", "")
                item_loader.add_value("external_id", external_id)

            title = response.xpath(f"(//div[@class='infos']//h3/text())[{i}]").get()
            if title:
                item_loader.add_value("title", title)
            
            rent = response.xpath(f"(//div[@class='infos']//h2/div/text()[contains(.,'€')])[{i}]").get()
            if rent:
                rent = rent.split("€")[0].replace(",", "").strip()
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            
            if response.xpath(f"(//div[@class='infos']//h3/text()[contains(.,'Pièce')])[{i}]").get():
                room_count = response.xpath(f"(//div[@class='infos']//h3/text()[contains(.,'Pièce')])[{i}]").get()
                room_count = room_count.split("- ")[-1].split(" ")[0]
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath(f"(//div[@class='infosPrinc']/div/text()[contains(.,'Chambre')])[{y}]").get()
                if room_count:
                    room_count = room_count.split(" ")[0]
                    item_loader.add_value("room_count", room_count)
                else:
                    pass
            
            bathroom_count = response.xpath(f"(//div[@class='infosPrinc'])[{y}]/div/text()[contains(.,'bain')]").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split(" ")[0]
                item_loader.add_value("bathroom_count", bathroom_count)
            
            square_meters = response.xpath(f"(//div[@class='infosPrinc']/div/text()[contains(.,'m²')])[{y}]").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.replace("m²","").split())
            
            latitude = response.xpath(f"(//div[@class='localisation_map'])[{i}]/input[@class='POI-lat']/@value").get()
            if latitude:
                item_loader.add_value("latitude", latitude)
            longitude = response.xpath(f"(//div[@class='localisation_map'])[{i}]/input[@class='POI-long']/@value").get()
            if longitude:
                item_loader.add_value("longitude", longitude)

            energy_label = response.xpath(f"((//div[@id='diagnostic'])[{y}]/div/img/@alt)[1]").get()
            if energy_label:
                energy_label = energy_label.replace("DPE-", "")
                if energy_label and "y" not in energy_label.lower():
                    item_loader.add_value("energy_label", energy_label)

            desc = response.xpath(f"((//div[@class='flag flag_fr'])[{y}]/following-sibling::text())[1]").get()
            if desc:           
                item_loader.add_value("description", desc)

            images = [x for x in response.xpath(f"(//div[@class='swiper-container'])[{i}]/div/meta/@content").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            item_loader.add_value("landlord_name", "Cabinet Lagadeuc")
            item_loader.add_value("landlord_phone", "02 35 15 72 72")
            item_loader.add_value("landlord_email", "location@lagadeuc.fr")
            yield item_loader.load_item()
