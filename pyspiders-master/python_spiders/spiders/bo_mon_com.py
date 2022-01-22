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
    name = 'bo_mon_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Bo_Mon_PySpider_france"
    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.bo-mon.com/a-louer/appartements/1", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.bo-mon.com/a-louer/maisons-villas/1", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//ul[@class='listingUL']/li//a[contains(.,'Voir le bien')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        if page == 2 or seen:
            follow_url = response.url.replace("/" + str(page - 1), "/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), 'page': page + 1})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//li[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(" ")[1])
            
        zipcode = response.xpath("//p/span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        city = response.xpath("//p/span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("address", city.strip())
            item_loader.add_value("city", city.strip())
        
        square_meters = response.xpath("//p/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p/span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p/span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//p/span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//p/span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//p/span[contains(.,'Charge')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        floor = response.xpath("//p/span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        furnished = response.xpath("//p/span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
            
        balcony = response.xpath("//p/span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
            
        terrace = response.xpath("//p/span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
                
        item_loader.add_value("landlord_name", "DIVES SUR MER IMMOBILIER")
        item_loader.add_value("landlord_phone", "05 55 79 66 00")
        item_loader.add_value("landlord_email", "contact@bo-mon.com")
        
        yield item_loader.load_item()