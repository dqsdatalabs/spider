# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'gerance_dracenoise_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Gerance_Dracenoise_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED":False}
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.agencedracenoise.com/annonces/location?page=1",
                ],
                "property_type": "apartment"
            },
	        # {
            #     "url": [
            #         "https://www.agencedracenoise.com/annonces/location?page=2",
                    
            #         ],
            #     "property_type": "apartment"
            # },
        ]  # LEVEL 1
        
        for url in start_urls:
            print(url.get("url"))
            item = url.get("url")[0]
            yield Request(
                url=item,
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.xpath("//a[contains(text(),' Détails de')]/@href").getall():
            # follow_url = response.urljoin(item.xpath("./@href").get())
            # print(follow_url)
            # status = item.xpath(".//span[@class='label']/text()").get()
            # if "agreed" in status.lower() or "under" in status.lower():
            #     continue
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:

            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Gerance_Dracenoise_PySpider_france")
        item_loader.add_value("external_id", response.url.split("-")[-1])

        title = response.xpath("//title/text()").get()
        if title:
            if "park" in title.lower():
                return
            item_loader.add_value("title", title.strip())
            
            zipcode = re.search("[\d]+",title)[0]
            item_loader.add_value("zipcode",zipcode)

        address = "".join(response.xpath("//span[contains(.,'ville')]//parent::div/text()").getall())
        region = "".join(response.xpath("//span[contains(.,'région')]//parent::div/text()").getall())
        if address and region:
            item_loader.add_value("address", address.strip() +"-" + region.strip())
        else:
            item_loader.add_value("address",address.strip())

        city = "".join(response.xpath("//span[contains(.,'ville')]//parent::div/text()").getall())
        if city:
            item_loader.add_value("city", city.strip())

        # zipcode = "".join(response.xpath("//h2//text()").getall())
        # if zipcode:
        #     zipcode = zipcode.split("(")[1].split(")")[0].strip()
        #     item_loader.add_value("zipcode", zipcode)

        square_meters = "".join(response.xpath("//span[contains(.,'surface')]//parent::div/text()").getall())
        if square_meters:
            square_meters = square_meters.split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(text(),'€')]/text()").get()
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//div[contains(@class,'legal')]//text()").get()
        if utilities and "dont" in utilities:
            utilities = utilities.split("€")[0].split("dont")[1].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[@class='w-full md:w-1/2 lg:w-2/3 text-gray-600 p-4']//text()").getall())
        if desc:
            # desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//span[contains(.,'pièces') or contains(.,'pièce')]//parent::div/text()").getall())
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//span[contains(.,'Pièce')]//parent::div/text()").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = "".join(response.xpath("//span[contains(.,'Salle')]//parent::div/text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//span[contains(.,'Parking')]//parent::div/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]//parent::div/text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = "".join(response.xpath("//span[contains(.,'étages')]//parent::div//text()").getall())
        if floor:
            floor = floor.split(":")[1].replace("ème","").replace("er","").strip()
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@class,'dpe')]//div[contains(@class,'letter')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//h3[contains(@class,'agency-name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'agency-phone')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_email", "gestion@agencedracenoise.com")

        yield item_loader.load_item()