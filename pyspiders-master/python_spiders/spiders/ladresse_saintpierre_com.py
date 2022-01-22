# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'ladresse_saintpierre_com'


    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.ladresse-saintpierre.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.ladresse-saintpierre.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17&C_27_tmp=2&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=",
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    execution_type='testing'
    country='france'
    locale='fr'
    def parse(self, response):

        for item in response.xpath("//div[@class='products-img']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//span[.='>']/../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Ladresse_saintpierre_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        meters = "".join(response.xpath("//ul/li[2]/span[@class='critere-value']/text()[contains(.,'m²')]").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0])
        
        
        room = "".join(response.xpath("//ul/li/span[@class='critere-value']/text()[contains(.,'Pièces')]").extract())
        if room:
            item_loader.add_value("room_count", room.split("Pièces")[0])
        
        
        price = "".join(response.xpath("//span[@class='hono_inclus_price']/text()[not(contains(.,'**'))]").extract())
        if price :
            item_loader.add_value("rent_string", price.strip())
        
        
        address = "".join(response.xpath("//div[@class='row']/h1/text()").extract())
        if address:
            addr = address.split("à")[1]
            item_loader.add_value("address", addr.strip())
            item_loader.add_value("city", addr.strip())
        
        
        external_id = "".join(response.xpath("//span[@itemprop='name']/text()[contains(.,'Ref.')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        desc = "".join(response.xpath("//div[@class='content-desc']/text()").extract())
        item_loader.add_value("description", desc.strip())

        label = "".join(response.xpath("//div[@class='product-dpe'][1]/div/@class").extract())
        if label :
            item_loader.add_value("energy_label", label.split(" ")[1].split("-")[1].upper())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='sliders-product']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        furnished=response.xpath("//ul/li/span[@class='critere-value']/text()[contains(.,'Meublée')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        parking=response.xpath("//ul/li/span[@class='critere-value']//preceding::img/@src[contains(.,'garage')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude=response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('lat:')[1].split(',')[0].strip()
            lng=latitude_longitude.split('lng:')[1].split(',')[0].strip()
            if lat or lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude", lng)

        item_loader.add_value("landlord_phone", "06 24 29 69 74")
        item_loader.add_value("landlord_name", "Ladresse Saintpierre")
         
        yield item_loader.load_item()
