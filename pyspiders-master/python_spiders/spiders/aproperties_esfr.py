# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import json

class MySpider(Spider):
    name = 'aproperties_esfr'
    execution_type='testing'
    country='spain'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.aproperties.es/fr/search?view=&mod=rental&q=&type%5B%5D=1&zone=3&area=&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=","property_type": "house","address" : "Valencia"},
            {"url": "https://www.aproperties.es/fr/search?view=&mod=rental&q=&type%5B%5D=14&zone=3&area=&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=","property_type": "apartment","address" : "Valencia"},
            {"url": "https://www.aproperties.es/fr/search?view=&mod=rental&q=&type%5B%5D=9&zone=3&area=&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=","property_type": "apartment","address" : "Valencia"},
            {"url": "https://www.aproperties.es/fr/search?view=&mod=rental&q=&type%5B%5D=10&zone=3&area=&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=","property_type": "house","address" : "Valencia"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),'address': url.get('address')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'propertyBlock')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),'address': response.meta.get('address')})
        pagination = response.xpath("//ul[@class='pagination']/li[@class='pagination__next']/a/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type'),'address': response.meta.get('address')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Aproperties_esfr_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[contains(@class,'contenido')]/h2/text()")

        price = response.xpath("//div[@class='_flex _flex--aib']/span/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price)

        item_loader.add_value("address", response.meta.get('address'))
        item_loader.add_value("city", response.meta.get('address'))

        meters = "".join(response.xpath("//ul/li[contains(.,'m²')]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.strip().split("m²")[0])

        room ="".join(response.xpath("//span[@class='property-specs__rowItem rooms']/strong/text()").extract())
        if room:
            item_loader.add_value("room_count",room )

        images = [response.urljoin(x)for x in response.xpath("//span[@class='propertyGallery__playbtn']/img/@src[. !='/templates/images/play.png']").extract()]
        if images:
            item_loader.add_value("images", images)
        desc = "".join(response.xpath("//div[@class='content']//text()").extract())
        desc = desc.replace('\n', '').replace('\r', '').replace('\t', '').replace('\xa0', '')
        item_loader.add_value("description", desc.strip())

        external_id ="".join(response.xpath("//div[@class='title']/span/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(".")[1])

        elevator=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Ascensor')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Piscina')]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        terrace=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Terraza')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        parking= "".join(response.xpath("//ul/li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[@class='content']//text()[contains(.,'balcón')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_phone", "93 528 89 08")
        item_loader.add_value("landlord_name", "Aproperties Es")

       
        yield item_loader.load_item()