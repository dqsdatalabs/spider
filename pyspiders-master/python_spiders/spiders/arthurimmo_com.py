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
    name = 'arthurimmo_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Arthurimmo_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.arthurimmo.com/immobilier/pays/locations/bien-appartement/france.htm",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.arthurimmo.com/immobilier/pays/locations/bien-maison/france.htm",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(url=item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        prop_type= response.meta.get('property_type')
        seen = False
        for item in response.xpath("//a[@class='before:empty-content before:absolute before:inset-0 before:z-0']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if prop_type and "apartment" in prop_type.lower():
            if page == 2 or seen:
                url = f"https://www.arthurimmo.com/immobilier/pays/locations/bien-appartement/france.htm?page={page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
        elif prop_type and "house" in prop_type.lower():
            if page == 2 or seen:
                url = f"https://www.arthurimmo.com/immobilier/pays/locations/bien-maison/france.htm?page={page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//div[@class='text-gray-800']//text()[contains(.,'Référence')]").get()
        if external_id:
            external_id = external_id.split("Référence")[1]
            if "-" in external_id:
                external_id=external_id.split("-")[0].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title", title.replace(" ",""))

        # address = "".join(response.xpath("//h1//span//text()").getall())
        # if address:
        #     item_loader.add_value("address", address.strip())

        # city = response.xpath("//h1//span//text()").get()
        # if city:
        #     item_loader.add_value("city", city.strip())

        # zipcode = response.xpath("//h1//span//span//text()").get()
        # if zipcode:
        #     zipcode = zipcode.replace("(","").replace(")","")
        #     item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//div[contains(.,'Surface habitable')]/following-sibling::div//text()[contains(.,'m²')]").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            square_meters = square_meters.replace(" ","")
            item_loader.add_value("square_meters", square_meters)

        rent = "".join(response.xpath("//div[@class='my-4 text-2xl text-gray-800 font-semibold']//text()").getall())
        if rent:
            rent = rent.split("€")[0]
            rent = rent.replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//ul[@class='text-gray-800 space-y-2 max-w-md']//li//div[contains(.,'Dépôt de garantie')]/following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.split("€")[0]
            deposit = deposit.replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//ul[@class='text-gray-800 space-y-2 max-w-md']//li//div[contains(.,'Charges')]/following-sibling::div//text").get()
        if utilities:
            utilities = utilities.split("€")[0]
            utilities = utilities.replace(" ","")
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[@x-init='clamped = $el.scrollHeight > $el.clientHeight']//text()").getall())
        if desc:
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Nombre de pièces')]/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Nombre de chambres')]/following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("(//li[@class='flex items-center justify-between']//div[contains(.,'Nombre de salles')]/following-sibling::div//text())[1]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//img[contains(@class,'object-cover overflow-hidden object-center h-full w-full')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        terrace = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Terrasse')]/following-sibling::div//text()[contains(.,'Oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Meublé')]/following-sibling::div//text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("(//li[@class='flex items-center justify-between']//div[contains(.,'étages')]/following-sibling::div//text())[1]").get()
        if floor:
            item_loader.add_value("floor", floor)
               
        yield item_loader.load_item()