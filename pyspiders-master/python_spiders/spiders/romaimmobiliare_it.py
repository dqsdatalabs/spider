# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'romaimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Romaimmobiliare_PySpider_italy"

    def start_requests(self):
        formdata = {
            "hactionFiltri": "ricerca",
            "hZoneSelezionate": "",
            "riFiltroContratto": "affitto",
            "riFiltroRif": "",
            "filtri_select_ListaTipologie": "1",
            "filtri_select_ListaRegioniStati": "",
            "filtri_select_ListaProvince": "",
            "filtri_select_ListaComuni": "",
            "riFiltroPrezzoDa": "",
            "riFiltroPrezzoA": "",
            "riFiltroMetriDa": "",
            "riFiltroMetriA": "",
            "riFiltroLocaliDa": "",
            "riFiltroLocaliA": "",
        }
        url = "http://www.romaimmobiliare.it/risultati-della-tua-ricerca.html"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )


    def parse(self, response):
        for item in response.xpath("//tr[@class='trRigaAnnuncio']/td/table"):
            data_id = item.xpath("./@data-id").get()
            url = f"http://www.romaimmobiliare.it/index.php?option=com_content&idscheda={data_id}&view=article&id=121&Itemid=279"
            yield Request(url, callback=self.populate_item)

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        prop = " ".join(response.xpath("//li[span[.='Tipologia']]/p/text()").extract())
        if get_p_type_string(prop):
            item_loader.add_value("property_type", get_p_type_string(prop))
        else: 
            return
        address = ", ".join(response.xpath("//li[span[.='Provincia']]/p/text() | //li[span[.='Regione']]/p/text() | //li[span[.='Zona']]/p/text()").getall())
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("title",address)
   
        item_loader.add_xpath("external_id", "//li[span[.='Riferimento']]/p/text()")
        item_loader.add_xpath("rent_string", "//li[span[.='€']]/p/text()")
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath("city", "//li[span[.='Provincia']]/p/text()")
        item_loader.add_xpath("square_meters", "//li[span[.='M²']]/p/text()")
        item_loader.add_xpath("bathroom_count", "//li[span[.='Bagni']]/p/text()[.!='0']")
        item_loader.add_xpath("energy_label", "//li[span[.='Cert. Energ.']]/p/text()")
        room_count = response.xpath("//li[span[.='Locali']]/p/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])

        description = " ".join(response.xpath("//div[@class='divImmagini']//text()").getall())
        if description:
            item_loader.add_value("description", description.split("Tel ")[0].split("Telefono")[0].strip())

        available_date = response.xpath("//li[span[.='Data']]/p/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
     
        item_loader.add_xpath("landlord_name", "//li[span[.='Agenzia']]/p/text()")

        phone = response.xpath("//li[span[.='Riferimenti']]/p/text()[contains(.,'Tel')]").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[-1].strip())
        else:
            item_loader.add_value("landlord_phone", "06.68210070")

        email = response.xpath("//li[span[.='Riferimenti']]/p/text()[contains(.,'mail')]").get()
        if email:
            item_loader.add_value("landlord_email", email.split(":")[-1].strip())


        images = response.xpath("//img[@class='thumb']/@src").getall()
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "camere" in p_type_string.lower():
        return "room"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartamento" in p_type_string.lower() or "attico" in p_type_string.lower() or "loft" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "casa" in p_type_string.lower()):
        return "house"
    else:
        return None