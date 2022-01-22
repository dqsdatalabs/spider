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
    name = 'volpescase_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Volpescase_PySpider_italy"
    start_urls = ['https://www.volpescase.it/r/annunci/affitto-.html?Codice=&macroCat=1&reddito=0&localita=&Provincia=0&Comune=0&Zona%5B%5D=0&Motivazione%5B%5D=2&cf=yes']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//section/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[strong[contains(.,'Tipologia')]]/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Codice')]//following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Provincia')]//following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.split(":")[1])

        address = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Zona')]//following-sibling::text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1])

        description = response.xpath(
            "//div[@class='testo']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Prezzo')]//following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent", rent.split(": €"))
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Totale mq')]//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1])

        bathroom_count = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Bagni')]//following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1])

        room_count = response.xpath(
            "//strong[contains(.,'Locali')]//following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1])

        utilities = response.xpath(
            "//strong[contains(.,'condominio')]//following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split()[-1])


        energy_label = response.xpath(
            "//div[@class='classe_energ']//div[@class='new_g']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        balcony = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Balconi')]//following-sibling::text()").get()
        if balcony and 'Presente'in balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        furnished = response.xpath(
            "//div[@class='informazioni']//div[@class='grid-6']//strong[contains(.,'Arredato')]//following-sibling::text()").get()
        if furnished and 'arredato'in furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='panel_th_sx']//ul[contains(@id,'images')]//li//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Volpes Case")
        phone_text = str(response.xpath("//div[@class='grid-6 all_sx']/p/text()").getall())
        if phone_text:
            landlord_phone = re.search("Telefono([ \(\)\d+.]+)'",phone_text).group(1)
            if landlord_phone:
                item_loader.add_value("landlord_phone",landlord_phone)
        email = response.xpath("//a[contains(@href,'@volpescase.it')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)

        position = response.xpath("//script[contains(text(),'var lat')]/text()").get()
        if position:
            lat = re.search('lat = "([\d\.]+)"',position).group(1)
            long = re.search('lgt = "([\d\.]+)"',position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None