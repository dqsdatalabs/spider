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
import dateparser

class MySpider(Spider):
    name = 'jouve_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.jouve-immobilier.fr/location-immobiliere-troyes/?typeBien=appartement&budgetMax=&nbChambre=&action=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.jouve-immobilier.fr/location-immobiliere-troyes/?typeBien=maison&budgetMax=&nbChambre=&action=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 24)
        
        seen = False
        for item in response.xpath("//div[@class='imgbien']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 24 or seen:
            p_url = response.url.split("&suite=")[0] + f"&suite={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+24}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Jouve_Immobilier_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//div[contains(@class,'description')]/h2/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[contains(@class,'content')]/p/text()[not(contains(.,'Réf'))]").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        rent = response.xpath("//div[@id='contourcadre']//div[@class='chiffre']/text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(" ",""))
            item_loader.add_value("currency", "EUR")
        
        room_count = "".join(response.xpath("//div[@id='contourcadre']//div[contains(.,'chambre')]//text()").getall())
        if room_count:
            room_count = room_count.strip().split("chambre")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//div[@id='contourcadre']//div[contains(.,'pièces')]//text()").getall())
            if room_count:
                room_count = room_count.strip().split("pièces")[0]
                item_loader.add_value("room_count", room_count)
        
        square_meters = "".join(response.xpath("//div[@id='contourcadre']//div[contains(.,'surface')]//text()").getall())
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        
        desc = "".join(response.xpath("//div[contains(@class,'detailannonce')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle de bain" in desc:
            bathroom_count = desc.split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "de garantie" in desc:
            deposit = desc.split("de garantie")[1].split("\u20ac")[0].replace(":","").strip()
            item_loader.add_value("deposit", deposit)
        
        # if "TTC dont" in desc:
        #     utilities = desc.split("TTC dont")[1].split("\u20ac")[0].replace(":","").strip().replace(",",".")
        #     item_loader.add_value("utilities", int(float(utilities)))
        if "de provision" in desc:
            utilities = desc.split("de provision")[0].split("+")[1].replace("\u20ac","").strip()
            item_loader.add_value("utilities", utilities)
        
        match = re.search(r'(\d+/\d+/\d+)', desc)
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
            
        images = [x for x in response.xpath("//div[contains(@class,'et_pb_gallery_item')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div[contains(@class,'content')]/p/text()[contains(.,'Réf')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip().split(" ")[0])
        
        energy_label = response.xpath("//div[@id='bloc-dpe']/@class").get()
        if energy_label:
            energy_label = energy_label.split("_")[1].capitalize()
            item_loader.add_value("energy_label", energy_label)
        
        elevator = response.xpath("//tr/td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name","Jouve Immobilier")
        item_loader.add_value("landlord_phone", "03 25 73 65 95")
        
        yield item_loader.load_item()