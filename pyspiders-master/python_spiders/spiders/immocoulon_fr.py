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
    name = 'immocoulon_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://agence-immobiliere-coulon.fr/location-maison-appartement-logement-immmobiliere-coulon-clermont-63"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'blocannonce location')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = "https://agence-immobiliere-coulon.fr/index.php?option=com_cfpi&view=search&layout=ajax&format=raw&task=display&transaction=Location&filters=eyJUeXBlVHJhbnNhY3Rpb24iOiJMb2NhdGlvbiIsIlR5cGVCaWVuIjoiIiwiU3VyZmFjZU1pbmkiOiIwIiwiU3VyZmFjZU1heGkiOiI1MDAiLCJCdWRnZXRNaW5pIjoiMCIsIkJ1ZGdldE1heGkiOiIxMDAwMDAwIiwiTG95ZXJNaW5pIjoiMCIsIkxveWVyTWF4aSI6IjUwMDAiLCJOYlBpZWNlc01pbmkiOiIwIiwiTmJQaWVjZXNNYXhpIjoiMjAiLCJDUCI6IiIsIlZpbGxlIjoiIiwiQ2xlQWZmYWlyZSI6IiIsIk1ldWJsZSI6IiIsIlBob3RvcyI6IiIsIlBhZ2UiOjJ9"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "page":page+1,
                }
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h1/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//p[@class='description']/text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        item_loader.add_value("external_source", "Immocoulon_PySpider_france")
        title = " ".join(response.xpath("//div[@class='soustitre']//text()[normalize-space()]").getall())
        if title:
            item_loader.add_value("title", title.strip())
        item_loader.add_xpath("external_id", "//p[@class='ref']/span/text()")
        item_loader.add_xpath("floor", "//p[text() ='Etage : ']/span/text()[.!='-']")
        address = response.xpath("//a[@id='cfpi_bouton_carte']/@onclick").get()
        if address:
            zipcode = address.split("','")[2]
            address = ", ".join(address.split("','")[1:]).split("'")[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
        city = response.xpath("//h1//text()").get()
        if city:
            city = city.split(" - ")[0].strip()
            item_loader.add_value("city", city)
            if not address:            
                item_loader.add_value("address", city)
   
        room_count = response.xpath("//p[text() ='Nb de chambres : ']/span/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[text() ='Nb de pièces : ']/span/text()[.!='0']")
      
        description = " ".join(response.xpath("//p[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//p[@class='surface']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
      
        item_loader.add_xpath("rent_string", "//p[@class='prix']/span/text()")
        deposit = response.xpath("//p[@class='depotgarantie']/span/text()[.!='-']").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//p[@class='charges']/span/text()[.!='-']").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
     
        available_date = response.xpath("//p[@class='disponibilite']/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        images = [response.urljoin(x) for x in response.xpath("//div[@class='sousvignettes']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)  
            
        item_loader.add_value("landlord_name", "L'immobiliere Coulon")
        item_loader.add_value("landlord_phone", "04 73 29 40 40")
        item_loader.add_value("landlord_email", "contact@immobiliere-coulon.com")

        energy_label = response.xpath("//p[text() ='Energie : ']/span/a/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("(")[0])
        balcony = response.xpath("//p[.='balcon']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        terrace = response.xpath("//p[.='terrasse']/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        elevator = response.xpath("//p[text() ='Ascenseur : ']/span/text()").get()
        if elevator:
            if "NON" in elevator.upper() or "SANS" in elevator.upper():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        yield item_loader.load_item()
            

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None