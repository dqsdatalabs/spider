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
    name = 'alouer86_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Alouer86_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.alouer86.fr/nos-appartements/",
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

        for item in response.xpath("//ul[@class='wpp_overview_data']//li[@class='property_title']//a//@href").extract():
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
            item_loader.add_value("title",title.replace("\u00b0","").replace("\u00e9",""))
             
        description=response.xpath("//div[@class='wpp_the_content']//p[contains(.,'Appartement ')]//following-sibling::p[1]//text()").getall()
        if description:
            item_loader.add_value("description",description)

        city=response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_quartier wpp_stat_plain_list_quartier alt')]//span[@class='value']//text()").get()
        if city:
            item_loader.add_value("city",city)

        address=response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_location wpp_stat_plain_list_location ')]//span[@class='value']//text()").get()
        if address:
            item_loader.add_value("address",address)

        square_meters="".join(response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_surface wpp_stat_plain_list_surface alt')]//span[@class='value']//text()").get())
        if square_meters:
            square_meters = square_meters.replace(".",",").split("M²")[0].strip()
            item_loader.add_value("square_meters",square_meters)

        rent=response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_loyer wpp_stat_plain_list_loyer')]//span[@class='value']//text()").get()
        if  rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        deposit=response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_deposit wpp_stat_plain_list_deposit alt')]//span[@class='value']//text()").get()
        if  deposit:
            item_loader.add_value("deposit",deposit)

        images=[response.urljoin(x) for x in response.xpath("//div[@class='wpp_the_content']//p//a//@href").getall()]
        if images:
            item_loader.add_value("images",images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(');')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)   

        landlord_phone=response.xpath("//ul[@id='property_stats']//li[contains(@class,'bienimmobilier_phone_number wpp_stat_plain_list_phone_number ')]//span[@class='value']//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", "JMC-Immo")
        yield item_loader.load_item()