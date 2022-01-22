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
    name = 'elite_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source= "Elite_PySpider_france"
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

        yield Request("https://elite.immo/recherche/",
                        callback=self.parse,
                        dont_filter=True,
                        )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='caption-footer col-md-12 col-xs-12 col-sm-4']//a//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type=response.xpath("//div[@class='offre_bien col-md-12 col-xs-12 col-sm-4']//div[@class='value']//span//text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "appartment")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title//text()")
        item_loader.add_xpath("external_id", "substring-after(//span[@class='ref']/text(),' ')")
        item_loader.add_xpath("zipcode", "normalize-space(//p[span[.='Code postal']]/span[2]/text())")
        item_loader.add_xpath("city", "normalize-space(//p[span[.='Ville']]/span[2]/text())")
        item_loader.add_value("address","{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))

        item_loader.add_xpath("bathroom_count","normalize-space(//p[span[contains(.,'Nb de salle d')]]/span[2]/text())")
        item_loader.add_xpath("floor","normalize-space(//p[span[contains(.,'Etage')]]/span[2]/text())")
        item_loader.add_xpath("latitude","substring-before(substring-after(//script//text()[contains(.,'center')],'lat: '),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(//script//text()[contains(.,'center')],'lng:  '),'}')")

        square_meters = "".join(response.xpath("substring-before(normalize-space(//p[span[.='Surface habitable (m²)']]/span[2]/text()),' ')").getall())
        if square_meters:
            s_meters = square_meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters)))

        room_count = "".join(response.xpath("//p[span[.='Nombre de chambre(s)']]/span[2]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = "".join(response.xpath("//p[span[.='Nombre de pièces']]/span[2]/text()").getall())
            if room_count:
                item_loader.add_value("room_count", "1")

        rent = "".join(response.xpath("//p[span[contains(.,'Loyer')]]/span[2]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))

        deposit = " ".join(response.xpath("//p[span[contains(.,'Dépôt')]]/span[2]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        utilities = "".join(response.xpath("//p[span[contains(.,'Charges ')]]/span[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        description = " ".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        furnished = "".join(response.xpath("//p[span[.='Meublé']]/span[2]/text()").getall())
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)

        terrace = "".join(response.xpath("//p[span[contains(.,'Terrasse')]]/span[2]/text()").getall())
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)

        elevator = "".join(response.xpath("normalize-space(//p[span[contains(.,'Ascenseur')]]/span[2]/text())").getall())
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("normalize-space(//p[span[contains(.,'Balcon')]]/span[2]/text())").getall())
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)


        parking = "".join(response.xpath("normalize-space(//p[span[contains(.,'Balcon')]]/span[2]/text())").getall())
        if parking:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "Elite Immo")
        item_loader.add_value("landlord_phone", "04 68 66 08 08")
        item_loader.add_value("landlord_email", "contact@elite.immo")

        yield item_loader.load_item()