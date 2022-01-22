# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek 
 
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from math import floor 

class MySpider(Spider):
    name = 'parisel_com'     
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.cogim-parisel.fr/louer/?type_bien%5B0%5D=appartement&prix_max&surface_min&nb_pieces", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.cogim-parisel.fr/louer/?type_bien%5B0%5D=maison&prix_max&surface_min&nb", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'annonce-in-list')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Parisel_PySpider_france")

        external_id = response.xpath("//div[contains(@id,'annonce')]//text()[contains(.,'Référence :')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        remove=item_loader.get_output_value("external_link")
        if "garage" in remove:
            return 

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            if "MEUBLE" in title.upper():
               item_loader.add_value("furnished", True) 
            address = ""
            city = ""
            if "DIJON" in title:
                address = "DIJON " + title.split("DIJON")[-1].strip()
                city = "Dijon"
            elif "A LOUER" in title:
                address = title.split("A LOUER")[-1].strip()
                address = " ".join(address.strip().split(" ")[1:])
                city = address
            elif "Quartier" in title:
                address = title.split("Quartier")[-1].strip()
                city = address
            if address:
                item_loader.add_value("address", address)
            else:
                address = response.xpath('//div[@class="secteur"]/text()').get()
                if address:
                    item_loader.add_value("address", address.strip())
            if city:
                item_loader.add_value("city", city)
          
        

        description = " ".join(response.xpath("//div[contains(@id,'annonce')]//p//text()").getall())
        if description:
            item_loader.add_value("description", description)

        rent = "".join(response.xpath("//p[contains(@class,'panel')][contains(.,'Loyer')]//text()").getall())
        if rent:
            rent = rent.split("Loyer charges comprises :")[1].split("€")[0].strip()
            item_loader.add_value("rent", rent.split(".")[0])
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//p[contains(@class,'panel')][contains(.,'Dépôt de garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split("Dépôt de garantie :")[1].split(".")[0].strip()
            item_loader.add_value("deposit", deposit)

        room_count = response.xpath("//td[contains(.,'chambre')]//following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//td[contains(.,'pièces')]//following-sibling::td//text()")

        square_meters = response.xpath("//td[contains(.,'Surface habitable')]//following-sibling::td//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())

        item_loader.add_xpath("bathroom_count", "//td[contains(.,'salle')]//following-sibling::td//text()")
     
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'swiper-slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Cabinet Parisel")
        item_loader.add_value("landlord_phone", "03 80 50 90 90")
        item_loader.add_value("landlord_email", "bonjour@cogim-parisel.fr")
        yield item_loader.load_item()