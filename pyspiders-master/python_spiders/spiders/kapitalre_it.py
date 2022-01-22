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
    name = 'kapitalre_it' 
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Kapitalre_PySpider_italy"
    start_urls = ['https://www.kapitalre.it/immobili/affitto']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(.,'Pagina successiva')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//div[contains(@class,'summary')]/text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//div[@class='field']//div[@class='field field--name-field-mt-prt-property-id field--type-string field--label-hidden field__item']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath(
            "//div[@class='field']//h5[@class='field__label']//following-sibling::text()").getall()
        if address:
            item_loader.add_value("address", address)
        city=response.xpath("//div[@class='sticky_side']/div[2]//text()").getall()
        if city:
            item_loader.add_value("city",city[-1].strip().split(" ")[1])
        zipcode=response.xpath("//div[@class='sticky_side']/div[2]//text()").getall()
        if zipcode:
            item_loader.add_value("zipcode",zipcode[-1].strip().split(" ")[0])


        description = response.xpath(
            "//div[@class='node__main-content col-12 col-md-8']//div[@class='clearfix text-formatted field field--name-field-mt-prt-body field--type-text-with-summary field--label-hidden field__item']//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[@class='col-lg-2 text-center']//div[@class='field field--name-field-mt-prt-price field--type-integer field--label-hidden field__item']//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€"))
        item_loader.add_value("currency", "EUR")
        floor=response.xpath("//h5[.='Piano']/following-sibling::div/text()").get()
        if floor and not "T" in floor:
            item_loader.add_value("floor",floor.split("/")[-1])

        square_meters = response.xpath(
            "//div[@class='field field--name-field-mt-prt-mq field--type-integer field--label-above']//h5[contains(.,'Superficie')]//following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        utilities=response.xpath("//section[@class='col-12 ']//label[contains(.,'Spese')]/following-sibling::div/text()").get()
        if utilities:
            utilities=utilities.split("/")[0].split("€")[1]
            if utilities:
                item_loader.add_value("utilities",utilities)

        bathroom_count = response.xpath(
            "//div[@class='field field--name-field-mt-prt-baths field--type-integer field--label-above']//h5[contains(.,'Bagni')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//div[@class='field field--name-field-mt-prt-rooms field--type-integer field--label-above']//h5[contains(.,'Locali')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        import dateparser
        available_date=response.xpath("//h1[@class='title']/span/text()").get()
        if available_date:
            date2 =  available_date.split("da")[-1].replace("da","").replace(".","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m")
                item_loader.add_value("available_date", date3)
        parking=response.xpath("//h1[@class='title']/span/text()").get()
        if parking and "parking" in parking:
            item_loader.add_value("parking",True)
        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='rev_slider']//ul//li[contains(@data-transition,'slidehorizontal')]//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        latitude=response.xpath("//h2[@class='location-title']/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split(",")[0])
        longitude=response.xpath("//h2[@class='location-title']/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split(",")[1].strip())

        item_loader.add_value("landlord_name", "KapitalRE.")
        item_loader.add_value("landlord_phone", "051 357200")
        item_loader.add_value(
            "landlord_email", "info@kapitalre.it")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None