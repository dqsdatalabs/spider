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
    name = 'tonusimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Tonusimmobiliare_PySpider_italy"
    start_urls = ['https://www.tonusimmobiliare.it/immobili-in-affitto/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='property-featured']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='out-link']/@href").get())
            status = item.xpath(".//span[@class='property-label']/text()").get()
            if status and ("nuovo" in status.lower() or "affitt" in status.lower()):
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.tonusimmobiliare.it/immobili-in-affitto/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        # status_error=response.xpath("//div[@id='error-404']//h2//text()").get()
        # if "not found" not in status_error:
        status=response.xpath("//span[@class='col-sm-5 detail-field-label status-label']//following-sibling::span//a[1]//text()").get()
        if status and "affittato" not in status.lower():
        
            item_loader.add_value("external_link", response.url)
            property_type = "".join(response.xpath("//div[contains(@class,'property-content')]//text()").getall())
            if get_p_type_string(property_type.strip()):
                item_loader.add_value("property_type", get_p_type_string(property_type.strip()))
            else:
                return
            item_loader.add_value("external_source", self.external_source)

            external_id = "".join(response.url)
            if "rif" in external_id:
                if "rif-" in external_id:
                    external_id=external_id.split("rif-")[-1].split("-")[0]
                    item_loader.add_value("external_id", external_id)
                else:
                    external_id=external_id.split("rif")[-1].split("-")[0]
                    item_loader.add_value("external_id", external_id)

            title = response.xpath(
                "//h1[@class='property-title']//text()").get()
            if title:
                item_loader.add_value("title", title)
            
            furnished = response.xpath(
                "//h1[@class='property-title']//text()").get()
            if "arredato" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

            city = response.xpath(
                "//div[@class='property-detail-content']//span[@class='col-sm-7 detail-field-value location-value']//text()").get()
            if city:
                item_loader.add_value("city", city)

            address = response.xpath(
                "//h1[@class='property-title']//small//text()").get()
            if address:
                item_loader.add_value("address", address)

            description = response.xpath(
                "//div[@class='property-content']//p//text()").getall()
            if description:
                item_loader.add_value("description", description)

            bathroom_count = response.xpath(
                "//div[@class='property-detail-content']//span[@class='value-_bathrooms col-sm-7 detail-field-value']//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

            room_count = response.xpath(
                "//div[@class='property-detail-content']//span[@class='value-_noo_property_field__locali col-sm-7 detail-field-value']//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

            square_meters = response.xpath(
                "//div[@class='property-detail-content']//span[@class='value-_area col-sm-7 detail-field-value']//text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("mq."))

            rent = response.xpath(
                "//div[@class='property-detail-content']//span[@class='col-sm-7 detail-field-value price-value']//text()").get()
            if "€uro" in rent:
                item_loader.add_value("rent", rent.split("€uro")[1])
            # else:
            #     rent = response.xpath(
            #         "//div[@class='property-detail-content']//span[@class='col-sm-7 detail-field-value price-value']//text()").getall()
            #     if rent:
            #         item_loader.add_value("rent", rent.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")


            utilities = response.xpath(
                "//div[@class='property-detail-content']//span[@class='value-_noo_property_field__spesecondominio col-sm-7 detail-field-value']//text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€uro")[-1].split("mensili")[0])

            latitude = response.xpath(
                "//div[@class='property-map-box']//@data-latitude").get()
            if latitude:
                item_loader.add_value("latitude", latitude)

            longitude = response.xpath(
                "//div[@class='property-map-box']//@data-longitude").get()
            if longitude:
                item_loader.add_value("longitude", longitude)

            images = [response.urljoin(x) for x in response.xpath(
                "//div[@class='images']//div[contains(@class,'caroufredsel')]//ul//li//a[contains(@class,'noo-lightbox-item')]//img//@src").getall()]
            if images:
                item_loader.add_value("images", images)

            item_loader.add_value("landlord_name", "Tonus Immobiliare")
            item_loader.add_value("landlord_phone", "(+39) 340 9829055")
            item_loader.add_value(
                "landlord_email", "info@tonusimmobiliare.it")

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