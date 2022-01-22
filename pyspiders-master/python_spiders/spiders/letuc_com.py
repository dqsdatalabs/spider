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
    name = 'letuc_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Letuc_PySpider_france"
    start_urls = ['https://www.letuc.com/location/1']  # LEVEL 1

    # 1. FOLLOWING 
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='property__link']/@href").getall():
            if "location" in item:
                follow_url = response.urljoin(item)
                if get_p_type_string(response.urljoin(item)):
                    yield Request(
                        follow_url, 
                        callback=self.populate_item, 
                        meta={
                            "property_type": get_p_type_string(response.url),
                        }
                    )
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.letuc.com/location/{page}"
            yield Request(
                url, 
                callback=self.parse,
                meta={
                    "page": page+1,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//div[@class='detail-3__reference js-animate']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        property_type=response.xpath("//a[.='Appartement']").get()
        if property_type:
            item_loader.add_value("property_type","apartment")
        property_type=response.xpath("//a[.='Maison'] | //a[.='Duplex'] | //a[.='Maison de village'] | //a[.='Villa']").get()
        if property_type:
            item_loader.add_value("property_type","house")
        
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        city=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if city: 
            item_loader.add_value("city",city)
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        adres=city+" "+zipcode
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//span[.=' CC*']/preceding-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//span[.='Surface habitable (m²)']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        room_count=response.xpath("//span[.='Nombre de pièces']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//title[.='Nombre de salles de bain']//parent::svg/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=response.xpath("//div[@class='detail-3__slide-2 slider-img']//a//@href").getall()
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//div[@class='detail-3__map']/div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//div[@class='detail-3__map']/div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("landlord_name","LETUC IMMO")
        
        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "duplex" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None