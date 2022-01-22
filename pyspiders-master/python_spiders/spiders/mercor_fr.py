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
    name = 'mercor_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Mercor_PySpider_france'    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mercor.fr/location/appartement?prod.prod_type=appt",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mercor.fr/location/maison?prod.prod_type=house",
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

        for item in response.xpath("//a[@class='_gozzbg']//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//div/h1//text()").get()
        if title:
            item_loader.add_value("title",title)
        address = response.xpath("//div[span[.='Localisation']]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        depo_util = response.xpath("//span[contains(.,'Provision sur charges')]/text()").get()
        if depo_util:
            item_loader.add_value("deposit", depo_util.lower().split("de garantie")[-1].split("€")[0].split(".")[0].strip())
            item_loader.add_value("utilities", depo_util.lower().split("provision sur charges")[-1].split("€")[0].split(".")[0].strip())

        zipcode = response.xpath("//script[contains(.,'postalCode')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('postalCode":"')[1].split('"')[0].strip())
        
        item_loader.add_xpath("floor", "//div[span[.='Étage']]/span[2]/text()")    
        item_loader.add_xpath("external_id", "//div[span[.='Référence']]/span[2]/text()")    
            
        rent = "".join(response.xpath("//div/p[contains(.,'€')]//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))     
    
        square_meters = response.xpath("//div[span[contains(.,'Surface :')]]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)     

        room_count = response.xpath("//div[span[.='Chambres']]/span[2]/text()").get()
        if room_count:        
            item_loader.add_value("room_count",room_count.strip())

        bathroom_count = response.xpath("//div[span[contains(.,'Salle de bains')]]/span[2]/text()[not(contains(.,'m'))]").get()
        if bathroom_count:        
            item_loader.add_value("bathroom_count",bathroom_count.strip())
     
        parking = response.xpath("//div[span[contains(.,'Stationnement ')]]/span[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//div[span[contains(.,'terrasse')]]/span[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[span[.='Ameublement']]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "meublé" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[span[contains(.,'Ascenseur')]]/span[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        swimming_pool = response.xpath("//div[span[.='Piscine']]/span[2]/text()").get()
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            elif "oui" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
        balcony = response.xpath("//div[span[contains(.,'balcon')]]/span[2]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        json_image = response.xpath("//div[@id='root']/script[@type='application/ld+json']//text()").get()
        data = json.loads(json_image)
        image = data["offers"]["itemOffered"]["photo"]
        images = [x["url"] for x in image]
        if images:
            item_loader.add_value("images", images)
       
        desc = " ".join(response.xpath("//div[@class='_17wyal9']//p[contains(@class,'_8r3m8p _5k1wy')]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())         
        
        if not item_loader.get_collected_values("description"):
            description = " ".join(response.xpath("//span[@class='_8r3m8p _5k1wy textblock ']//text()").getall()).strip()
            if description: item_loader.add_value("description", description)    
     
        item_loader.add_xpath("landlord_name", "//div[@class='_q2jpep']/span/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='_q2jpep']//span[div[span[@title='fas-phone']]]/text()")    
    
        yield item_loader.load_item()