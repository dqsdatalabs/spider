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
    name = 'jfg_immobilier_fr'
    execution_type='testing' 
    country='france'
    locale='fr'
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
                    "https://www.jfg-immobilier.fr/a-louer/appartements/1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.jfg-immobilier.fr/a-louer/maisons-villas/1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//span[contains(text(),'Voir le bien')]/../@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:            
            follow_url = f"https://www.jfg-immobilier.fr/a-louer/appartements/{page}"
            yield Request(follow_url,  callback=self.parse, meta={"page": page + 1, "property_type": response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Jfg_Immobilier_PySpider_france")      
        external_id = response.xpath("//li[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[-1].strip())
        title = response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title))
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[span[contains(.,'Nombre de pièce')]]/span[2]/text()")

        item_loader.add_xpath("bathroom_count", "//p[span[contains(.,'Nb de salle d')]]/span[2]/text()")
        zipcode = response.xpath("//p[span[.='Code postal']]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//p[span[.='Ville']]/span[2]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())
        item_loader.add_xpath("floor", "//p[span[.='Etage']]/span[2]/text()")        
        square_meters = response.xpath("//p[span[.='Surface habitable (m²)']]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip().replace(",","."))))
      
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//p[span[.='Meublé']]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//p[span[.='Balcon']]/span[2]/text()").get()
        if balcony:
            if balcony.lower().strip() =="non":
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        terrace = response.xpath("//p[span[.='Terrasse']]/span[2]/text()").get()
        if terrace:
            if terrace.lower().strip() =="non":
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        elevator = response.xpath("//p[span[.='Ascenseur']]/span[2]/text()").get()
        if elevator:
            if elevator.lower().strip() =="non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking = response.xpath("//p[span[.='Nombre de garage' or .='Nombre de parking']]/span[2]/text()").get()
        if parking:
            if parking.lower().strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        rent = response.xpath("//p[span[.='Loyer CC* / mois']]/span[2]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//p[span[.='Dépôt de garantie TTC']]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//p[span[contains(.,'Charges locatives')]]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        item_loader.add_value("landlord_name", "JFG IMMOBILIER")
        item_loader.add_value("landlord_phone", "06 19 70 15 76")
        item_loader.add_value("landlord_email", "delphinefonty@jfg-immobilier.fr")
        yield item_loader.load_item()