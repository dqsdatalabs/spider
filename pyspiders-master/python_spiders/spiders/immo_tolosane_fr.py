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
    name = 'immo_tolosane_fr' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Immo_Tolosane_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.immo-tolosane.fr/recherche/",
                "property_type": "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property__content-wrapper']/a/@href").extract():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        if "fr/vente" in response.url:
            return 

        external_id = response.xpath("//div[@class='detail-1__reference'][contains(.,'Réf')]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        else:
            item_loader.add_xpath("external_id", "substring-after(//span[@class='ref']/text(),'Ref ')")
        
        title = response.xpath("//div[@class='main-info__content-wrapper']/header/div/span/text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(.,'Ville')]//following-sibling::span//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(.,'Ville')]//following-sibling::span//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//span[contains(.,'Code')]//following-sibling::span//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//span[contains(.,'Surface habitable')]//following-sibling::span//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(",")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(.,'Loyer')]//following-sibling::span//text()").get()
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()[not(contains(.,'Non'))]").get()
        if deposit:
            deposit = deposit.strip().replace("€","").replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//span[contains(.,'Charge')]//following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.strip().replace("€","").replace(" ","")
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[@class='detail-1__text']/p/span/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'pièce')]//following-sibling::span[contains(@class,'value')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//span[contains(.,'salle')]//following-sibling::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[@class='swiper-wrapper js-lightbox-swiper']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//span[contains(.,'Etage')]//following-sibling::span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        latitude = response.xpath("//div[@class='detail-1__map']/div/@data-lat").get()
        if latitude:  
            item_loader.add_value("latitude", latitude)            
        longitude = response.xpath("//div[@class='detail-1__map']/div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "IMMOBILIERE TOLOSANE")
        item_loader.add_value("landlord_phone", "05 61 00 47 11")
        item_loader.add_value("landlord_email", "contact@immo-tolosane.fr")

        yield item_loader.load_item()