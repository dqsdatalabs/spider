# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = 'duprazimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Duprazimmobilier_PySpider_france_fr"

    def start_requests(self):

        yield Request("https://www.dupraz-immobilier.com/location/1",callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-listing-v3__item item']"):
            follow_url = response.urljoin(item.xpath(".//a[span[.='Voir le bien']]/@href").get())
          
            prop_type = item.xpath(".//h2//text()").get()
            if get_p_type_string(prop_type): 
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': get_p_type_string(prop_type)})
          
    # 2. SCRAPING level 2
    def populate_item(self, response):
        print(response.url)
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        if response.url == "https://www.dupraz-immobilier.com/":
            return
        prop_type = response.meta.get('property_type')
        property_type = response.xpath("//div[@class='head-offre-titre']/h2/text()").get()
        if property_type:
            if "STUDIO" in property_type.upper():
                prop_type = "studio"
        item_loader.add_value("property_type", prop_type)
        item_loader.add_xpath("title", "//h1//text()")
        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//div[@class='property-detail-v3__info-id']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())

        price = "".join(response.xpath("//div[@class='main-info__price price']/span[1]/text()").extract())
        if price:
            item_loader.add_value("rent_string", price.strip().replace(" ",""))

        deposit = "".join(response.xpath("//div[span[contains(.,'de garantie')]]/span[2]/text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ",""))

        utilities = "".join(response.xpath("//div[span[contains(.,'Charges locatives')]]/span[2]/text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(" ",""))

        label = "".join(response.xpath("substring-after(//div[@class='datas__energy']/img[@alt='DPE']/@src,'&idann=')").extract())
        if  label !="dpevierge":
            if label:
                e_label=energy_label_calculate(label)
                item_loader.add_value("energy_label",e_label )

        square = response.xpath("//div[span[contains(.,'Surface habitable')]]/span[2]/text()").get()
        if square:
            s_meters = square.split("m²")[0].replace(",",".")
            item_loader.add_value("square_meters",int(float(s_meters)))
        elif not square:                
            square = response.xpath("//div[span[contains(.,'Surface')]]/span[2]/text()").get()
            if square:
                s_meters = square.split("m²")[0].replace(",",".")
                item_loader.add_value("square_meters",int(float(s_meters)))

        item_loader.add_xpath("city", "//div[span[.='Ville']]/span[2]/text()")
        item_loader.add_xpath("address", "//div[span[.='Ville']]/span[2]/text()")
        
        item_loader.add_xpath("zipcode", "//div[span[.='Code postal']]/span[2]/text()")

        desc = "".join(response.xpath( "//div[@class='about__text-block text-block']//text()").extract())
        item_loader.add_value("description", desc)

  

        images = [response.urljoin(x)for x in response.xpath("//div[@class='swiper-slide slider-img__swiper-slide']//a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath( "floor", "//div[span[.='Etage']]/span[2]/text()")
        item_loader.add_xpath( "room_count", "//div[span[contains(.,'chambre')]]/span[2]/text()")
        item_loader.add_xpath( "bathroom_count", "//div[span[contains(.,'Nb de salle d')]]/span[2]/text()")

        parking = response.xpath("//div[span[contains(.,'parking')]]/span[2]/text()").get()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        elevator = response.xpath("//div[span[.='Ascenseur']]/span[2]/text()").extract_first()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        furnished = response.xpath("//div[span[contains(.,'Meublé')]]/span[2]/text()").extract_first()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[span[.='Terrasse']]/span[2]/text()").extract_first()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        balcony = response.xpath("//div[span[.='Balcon']]/span[2]/text()").extract_first()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)


        item_loader.add_value("landlord_phone", "04 88 92 72 28")
        item_loader.add_value("landlord_email", "thonon@dupraz-immobilier.com")
        item_loader.add_value("landlord_name", "Dupraz Immobilier")


        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower() or "duplex"  in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None