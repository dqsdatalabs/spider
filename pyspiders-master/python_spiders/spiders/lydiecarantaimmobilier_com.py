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


class MySpider(Spider):
    name = 'lydiecarantaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.lydiecaranta-immobilier.com/location/1", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='links-group__wrapper']/a/@href").extract():
            follow_url = response.urljoin(item)
            if "location" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            url = f"https://www.lydiecaranta-immobilier.com/votre-recherche/{page}"
            yield Request(
                url, 
                callback=self.parse, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                    "page" : page+1
                }
            )
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Lydiecarantaimmobilier_com_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        rent =  response.xpath("//div[@class='property-detail-v3__main-info main-info']/div[contains(@class,'price')]/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.strip())

        deposit =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Dépôt de garantie TTC']]/span[2]/text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0])

        floor =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Nombre de niveaux']]/span[2]/text()").extract())
        if floor:
            item_loader.add_value("floor", floor.strip())

        utilities =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Honoraires TTC charge locataire']]/span[2]/text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        meters = "".join(response.xpath("//div[@class='table-aria']//div[span[.='Surface habitable (m²)']]/span[2]/text()").extract())
        if meters:
            s_meters = meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", math.ceil(float(s_meters)))

        external_id = "".join(response.xpath("//div[@class='property-detail-v3__info-id']/text()").extract())
        item_loader.add_value("external_id", external_id.split(":")[1].strip())


        room_count =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Nombre de chambre(s)']]/span[2]/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Nombre de pièces']]/span[2]/text()").extract())
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Nb de salle de bains']]/span[2]/text()").extract())
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        desc = "".join(response.xpath("//div[@class='about__text-block text-block']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        city = response.xpath("//div[@class='table-aria']//div[span[.='Ville']]/span[2]/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())

        item_loader.add_xpath("address", "normalize-space(//span[@class='title-subtitle__subtitle']/text())")

        images = [x for x in response.xpath("//picture/source/@data-srcset").getall()]
        if images:
            item_loader.add_value("images", images)

        zipcode =  "".join(response.xpath("//div[@class='table-aria']//div[span[.='Code postal']]/span[2]/text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        furnished = response.xpath("//div[@class='table-aria']//div[span[.='Meublé']]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        balcony = response.xpath("//div[@class='table-aria']//div[span[.='Balcon']]/span[2]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//div[@class='table-aria']//div[span[.='Terrasse']]/span[2]/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        elevator = response.xpath("//p[@class='data']/span[contains(.,'Ascenseur')]/following-sibling::span[not(contains(.,'NON'))]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        parking = response.xpath("//div[@class='table-aria']//div[span[.='Nombre de parking']]/span[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_phone", '04 94 43 79 03')
        item_loader.add_value("landlord_email", 'contact@lydiecaranta-immobilier.com')
        item_loader.add_value("landlord_name", 'Lydiecaranta Immobilier')

        yield item_loader.load_item()