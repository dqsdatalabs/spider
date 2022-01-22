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
    name = 'dupinimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    scale_separator ='.'
    external_source= "Dupinimmobilier_PySpider_france_fr"
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
                "url": [
                    "http://www.dupinimmobilier.com/recherche/",
                ],
            }
        ]  # LEVEL 1


        for url in start_urls:
            for item in url.get('url'):
                print(item)
                yield Request(
                    url=item,
                    callback=self.parse
                )


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'contentlst')]/section[@class='listing2']/ul/li//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = response.xpath("//div[contains(@class,'bienTitle')]//h2/text()").get()
        property_type = ""
        if "Appartement" in prop_type:
            property_type = "apartment"
        elif "Maison" in prop_type:
            property_type = "house"
        elif "Studio" in prop_type:
            property_type = "apartment"
        elif "Duplex" in prop_type:
            property_type = "apartment"
        elif "Villa" in prop_type:
            property_type = "house"
        elif "Immeuble" in prop_type:
            property_type = "house"
        else:
            return 

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "normalize-space(//div[@class='bienTitle']/h2/text())")
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[contains(@class,'prix')]//text()[not(contains(.,'Prix'))]").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        square_meters=response.xpath("//span[contains(.,'Surface habitable')]//following-sibling::span[contains(.,'m²')]/text()").get()
        if square_meters:
            meters = square_meters.split('m²')[0]
            if square_meters and "," in square_meters:
                meters = square_meters.replace(",",".")
        item_loader.add_value("square_meters",meters)

        zipcode = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Code postal')]/span[2]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Ville')]/span[2]/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())
        
        room_count=response.xpath("//div[@class='tab-content']/div/p[contains(.,'pièces')]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count=response.xpath("//div[@class='tab-content']/div/p[contains(.,'Nb de salle d')]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        deposit = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Dépôt de garantie')]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        utilities = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Charges')]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0]
            item_loader.add_value("longitude", longitude.strip())
            item_loader.add_value("latitude", latitude.strip())
            
        external_id=response.xpath("//div[@class='bienTitle']/span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(' ')[1])

        desc="".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "DUPIN IMMOBILIER")
        item_loader.add_value("landlord_phone", "+33 (0) 4 67 73 34 68")
        item_loader.add_value("landlord_email", "agencedupin.immobilier@orange.fr")

        floor = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Etage')]/span[2]/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.strip())

        furnished = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Meublé')]/span[2]/text()").extract_first()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Terrasse')]/span[2]/text()").extract_first()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace",False )
            else:
                item_loader.add_value("terrace", True)

        balcony = response.xpath("//div[@class='tab-content']/div/p[contains(.,'Balcon')]/span[2]/text()").extract_first()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony",False )
            else:
                item_loader.add_value("balcony", True)

        parking = response.xpath("//div[@class='tab-content']/div/p[contains(.,'parking')]/span[2]/text()").extract_first()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking",False )
            else:
                item_loader.add_value("parking", True)
        
        
        yield item_loader.load_item()
