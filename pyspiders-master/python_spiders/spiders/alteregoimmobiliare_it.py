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
    name = 'alteregoimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Alteregoimmobiliare_Pyspider_italy"

    # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.alteregoimmobiliare.it/ricerca-immobili-vendite-e-locazioni/?status=locazioni&type=appartamento",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//section[@class='listing-layout property-grid']//article//figure//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        description=response.xpath("//div[@class='content clearfix']/p//text() | //div[@class='dsc']/text()").get()
        if description:
            item_loader.add_value("description",description)


        utilities=response.xpath("//table[contains(@class,'criso_table')]//tr//td[contains(.,'Spese condominiali:')]//following-sibling::td//text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)

        city="".join(response.xpath("//address[@class='title']//text()").get())
        if city:
            city="".join(city.split(",")[-3:-2])
            item_loader.add_value("city",city)

        zipcode="".join(response.xpath("//address[@class='title']//text()").get())
        if zipcode:
            zipcode="".join(zipcode.split(",")[-2:-1])
            item_loader.add_value("zipcode",zipcode)

        address=response.xpath("//address[@class='title']//text()").get()
        if address:
            item_loader.add_value("address",address)

        rent=response.xpath("//h5[@class='price']//span[@class='price-and-type']//i//following-sibling::text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        energy_label=response.xpath("//td[contains(.,'Classe Energetica')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("(")[0].split("tica")[1])
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("lat")[1].split(",")[0].replace(":","").replace('"',""))
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("lng")[1].split(",")[0].replace(":","").replace('"',""))

        square_meters=response.xpath("//table[contains(@class,'criso_table')]//tr//td[contains(.,'MQ ')]//following-sibling::td//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        room_count=response.xpath("//table[contains(@class,'criso_table')]//tr//td[contains(.,'vani')]//following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//table[contains(@class,'criso_table')]//tr//td[contains(.,'bagni')]//following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        images = [response.urljoin(x)for x in response.xpath("//div[@class='item']//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)
 
        item_loader.add_value("landlord_phone", "010-859.33.30")
        item_loader.add_value("landlord_name", "ALTEREGO IMMOBILIARE")

        furnished = response.xpath("//td[text()='Arredato:']/following-sibling::td/text()[.='Si']").get()
        if furnished:
            item_loader.add_value("furnished",True)


        yield item_loader.load_item()
