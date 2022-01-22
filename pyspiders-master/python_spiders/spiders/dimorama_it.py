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
    name = 'dimorama_it_disabled'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Dimorama_PySpider_italy"

    # 1. FOLLOWING
    def start_requests(self):
        start_url = "https://www.dimorama.it/"
        yield Request(start_url,callback=self.parse,)
    def parse(self, response):
        for item in response.xpath("//section[contains(@class,'list-location')]//dd/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.jump_city) #meta={"property_type": prop_type}
    
    def jump_city(self, response):
    
        for item in response.xpath("//li[@class='lista-comuni-nome']"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            city = item.xpath(".//text()").get()
            yield Request(follow_url, callback=self.parse_listing, meta={"city":city})

    def parse_listing(self, response):
        
        for item in response.xpath("//div[contains(@class,'results-single')]//a[@class='auction-title']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            property_type = item.xpath("./h3/text()").get()
            if get_p_type_string(property_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type),"city":response.meta.get('city')})
        
        next_page = response.xpath("//li[@class='page-item']/a[@title='Vai alla pagina successiva']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse_listing,
                meta={
                    "city":response.meta.get('city')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("city", response.meta.get('city'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//div[@class='notice text-lgrey']//text()[contains(.,'Riferimento:')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Riferimento:"))

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath(
            "//div[@class='col-12 details-descr ptb-15']//p[@class='mtb-10 fw-700 text-lgreen']//text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[1])

        address = response.xpath(
            "//div[@class='col-12 details-descr ptb-15']//p[@class='mtb-10 fw-700 text-lgreen']//text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1])

        description = response.xpath(
            "//div[@class='col-12 details-descr ptb-15']//p[@class='mtb-0 pt-10']//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[contains(@class,'col-12 col-sm-6 col-md-6 col-lg-12')]//span[@class='fs-32 fs-md-24 fw-600 results-prezzo text-dgreen results-prezzo text-dgreen']//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//ul[@class='auction-infos']//li//text()[contains(.,'Mq')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("Mq"))

        room_count = response.xpath(
            "//ul[@class='auction-infos']//li//text()[contains(.,'Locali')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Locali"))

        available_date = response.xpath(
            "//div[@class='col-12 col-sm-6 col-md-6 col-lg-12 order-2 order-lg-1 text-center text-lg-left pb-20']//span[@class='fs-16 fs-md-18 fw-400 text-horange']//text()").get()
        if available_date:
            item_loader.add_value("available_date", available_date.split("Data Asta:"))

        images = [response.urljoin(x) for x in response.xpath(
            "//div[contains(@class,'flexslider loading')]//ul[contains(@class,'slides')]//li//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        # item_loader.add_value("landlord_name", "Media Casa")
        # item_loader.add_value("landlord_phone", "0630893724")
        # item_loader.add_value(
        #     "landlord_email", "info@mediacasa.org")

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