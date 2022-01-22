# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math
class MySpider(Spider):
    name = 'eurovilla_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Eurovilla_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.eurovilla.pl/oferta/rodzaj/wynajem/typ/domy/?utm=click&na_strone=10&sortowanie=najnowsze_malejaco&cena_od=&cena_do=&powierzchnia_od=&powierzchnia_do=&pokoi_od=&pokoi_do=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.eurovilla.pl/oferta/rodzaj/wynajem/typ/mieszkania/?utm=click&na_strone=10&sortowanie=najnowsze_malejaco&cena_od=&cena_do=&powierzchnia_od=&powierzchnia_do=&pokoi_od=&pokoi_do=",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//h2[@class='entry-title']/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        if page == 2 or seen: 
            nextpage=response.xpath("//a[.='Następna strona']/@href").get() 
            if nextpage:      
                yield Request(
                    response.urljoin(nextpage),
                    callback=self.parse,
                    dont_filter=True,
                    meta={"page":page+1,"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        rent=response.xpath("//span[@class='nev-pa-offer-price-o']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ","").strip())
        item_loader.add_value("currency","PLN")
        title=response.xpath("//h1[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='entry-title']/following-sibling::p/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//span//sup[.='2']/parent::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].split("m")[0].strip())
        room_count=response.xpath("//span[contains(.,'pokoje')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        external_id=response.xpath("//span[contains(.,'nr')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        images=[x for x in response.xpath("//img[@class='owl-lazy property-image zoom']/@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-9 col-lg-9 ev_o_text']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        name=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12 tabelka']/h3/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12 tabelka']//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12 tabelka']//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)

        yield item_loader.load_item()