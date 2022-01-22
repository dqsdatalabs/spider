# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re

class MySpider(Spider):
    name = 'cabinetderocquigny_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinetderocquigny_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://cabinetderocquigny.com/advanced-search/?type=appartement&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "apartment"},
            {"url": "https://cabinetderocquigny.com/advanced-search/?type=maisonvilla&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "house"},
            {"url": "https://cabinetderocquigny.com/advanced-search/?type=maison&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "house"},
            {"url": "https://cabinetderocquigny.com/advanced-search/?type=duplex&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "apartment"},
            {"url": "https://cabinetderocquigny.com/advanced-search/?type=studio&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-listing')]//div[contains(@class,'LOCATION')]//div[contains(@class,'phone')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Cabinetderocquigny_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("rent_string","//div[@class='listing-price LOCATION']/span[@class='item-price']//text()")
        item_loader.add_xpath("external_id","//span/span[contains(.,'Référence')]/following-sibling::span/text()")
        item_loader.add_xpath("floor", "//div[@class='aivoni-details'][contains(.,'Etage')]/label/text()")

        zipcode = response.xpath("//li[@class='detail-zip'][contains(.,'Code Postal')]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//li[@class='detail-city']/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
        item_loader.add_xpath("address","//span/span[contains(.,'Lieu')]/following-sibling::span/text()")

        room_count = response.xpath("//div[@class='aivoni-details']/strong[contains(.,'Chambre')]/following-sibling::*/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0].strip())
        else:
            room_count = response.xpath("//h1/text()").re_first(r'(\d+)\s*pièce')
            if room_count:
                item_loader.add_value("room_count", room_count)

        utilities = response.xpath("//div[@class='aivoni-details']/strong[contains(.,'Charges')]/following-sibling::*/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0].strip())
        
        bathroom_count = response.xpath("//div[@class='aivoni-details']/strong[contains(.,'Salle')]/following-sibling::*/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square = response.xpath("//span/span[contains(.,'Surface')]/following-sibling::span/text()").extract_first()
        if square:
            square =square.split("m")[0].strip()
            square_meters = math.ceil(float(square.strip()))
            item_loader.add_value("square_meters",square_meters )

        deposit = response.xpath("//div[@class='aivoni-details'][contains(.,'Dépôt de Garantie ')]/label/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])
        
        energy_label = response.xpath("//div[contains(@class,'DPEBOX')]/h5[contains(.,'DPE')]/text()").extract_first()
        if energy_label:
            energy=energy_label.split(":")[1].split("(")[0]
            item_loader.add_value("energy_label",energy.strip())
        
               
        desc = "".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "piscine" in desc:
                item_loader.add_value("swimming_pool", True)
            if "terrasse" in desc:
                item_loader.add_value("terrace", True)
            if "meublé" in desc:
                item_loader.add_value("furnished", True)
   

        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider-thumbs']//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "02 38 53 57 42")
        item_loader.add_value("landlord_name", "Cabinet de Rocquigny")
        mail = response.xpath("//a[contains(@href, '@')]/@href").re_first(r'mailto:(.*)')
        if mail:
            item_loader.add_value('landlord_email', mail)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        yield item_loader.load_item()