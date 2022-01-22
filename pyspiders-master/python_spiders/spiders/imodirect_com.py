# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'imodirect_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'
    external_source="Imodirect_PySpider_france"

    def start_requests(self):
        yield Request("https://www.imodirect.com/Annonces/Annonces/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'overlay-container')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//h1/text()").getall())
        if get_p_type_string(p_type):
            item_loader.add_value("property_type", get_p_type_string(p_type))
        else:
            p_type = "".join(response.xpath("//p/b//text()").getall())
            if get_p_type_string(p_type):
                item_loader.add_value("property_type", get_p_type_string(p_type))
            else:
                return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//div[@class='row']/div/h1/text()")

        item_loader.add_xpath("zipcode", "//tr[td[.='Code postal']]/td[2]/text()")
        item_loader.add_xpath("city", "//tr[td[.='Ville']]/td[2]/text()")

        item_loader.add_xpath("address", "concat(//tr[td[.='Ville']]/td[2]/text(), ' ',//tr[td[.='Code postal']]/td[2]/text())")
        item_loader.add_xpath("rent_string", "//tr[td[.='Loyer']]/td[2]/text()")

        item_loader.add_xpath("deposit", "//tr[td[.='Dépôt de garantie']]/td[2]/text()")
        item_loader.add_xpath("utilities", "//tr[td[.='Charges mensuelles']]/td[2]/text()")

        item_loader.add_xpath("room_count", "//tr[td[.='Chambres']]/td[2]/text() | //tr[td[.='Séjour']]/td[2]/text()")
        item_loader.add_xpath("bathroom_count", "//tr[td[.='Salle de douche']]/td[2]/text()")
        item_loader.add_xpath("floor", "//tr[td[.='Étage / Niveau']]/td[2]/text()")

        description = " ".join(response.xpath("//div[@class='container']/p//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        label = " ".join(response.xpath("substring-before(//tr[td[.='Consommation énergétique']]/td[2]/text(),'(')").getall())  
        if label:
            item_loader.add_value("energy_label", label.strip())

        external_id = response.xpath("//div[@class='container']/p//text()[contains(.,'Réf') or contains(.,'Ref')]").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].split("-")[0].split("f")[-1].strip())

        square_meters = response.xpath("//tr[td[.='Surface habitable']]/td[2]/text()").extract_first()
        if square_meters:
            meters = square_meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(meters)))


        images = [x for x in response.xpath("//div[contains(@class,'mobile-sm')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date=response.xpath("//tr[td[.='Date de disponibilité']]/td[2]/text()").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        furnished = response.xpath("//tr[td[.='Meublé']]/td[2]/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished",False)


        elevator = response.xpath("//tr[td[.='Ascenseur']]/td[2]/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator",True)
            elif "non" in elevator.lower():
                item_loader.add_value("elevator",False)

        balcony = response.xpath("//tr[td[.='Balcon']]/td[2]/text()").get()
        if balcony:
            if balcony !="0":
                item_loader.add_value("balcony",True)
            else:
                item_loader.add_value("balcony",False)

        parking = "".join(response.xpath("//tr[td[.='Parking ']]/td[2]/text()").getall())
        if parking:
            if balcony !="0":
                item_loader.add_value("parking",True)
            else:
                item_loader.add_value("parking",False)

        terrace = "".join(response.xpath("//tr[td[.='Terrasses']]/td[2]/text()").getall())
        if terrace:
            item_loader.add_value("terrace",True)

        item_loader.add_value("landlord_phone", "09 80 80 38 38")
        item_loader.add_value("landlord_name", "Imodirect")
        item_loader.add_value("landlord_email", "presse@imodirect.com")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None