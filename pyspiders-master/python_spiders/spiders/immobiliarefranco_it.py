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
    name = 'immobiliarefranco_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Immobiliarefranco_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=1&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code=",
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=100007&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code="
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=136&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code=",
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=100008&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code=",
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=142&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code=",
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=144&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code=",
                    "https://www.immobiliarefranco.it/ita/immobili?order_by=date_update_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&tabs=on&coords=&coords_center=&coords_zoom=&rental=1&property_type_id=145&city_id=&size_min=&size_max=&price=%E2%82%AC0+-+%E2%82%AC2.000.000&price_max=&code="
                ],
                "property_type": "house"
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
        
        for item in response.xpath("//div[@class='card']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//span[@class='code']/text()").get()
        if external_id:
            external_id=external_id.split(".")[-1]
            external_id=re.findall("\d+",external_id)
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//span[.='Prezzo']/following-sibling::b/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("€",""))
        item_loader.add_value("currency","EUR")
        address=response.xpath("//span[@class='location']/text()").get()
        if address:
            item_loader.add_value("address",address)
            city=address.split(",")[0]
            item_loader.add_value("city",city)
        square_meters=response.xpath("//span[.='MQ']/following-sibling::b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        energy_label=response.xpath("//span[.='Classe Energ.']/following-sibling::b/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        desc=" ".join(response.xpath("//p[@class='description']/span/following-sibling::text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//div[@id='property-images']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        floor_plan_images=response.xpath("//div[@class='planimetries_list']/a/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images",floor_plan_images)
        latitude=response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude:
            latitude=latitude.split("mapOptions")[0].split("LatLng")[-1].split(",")[0].replace("(","")
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if longitude:
            longitude=longitude.split("mapOptions")[0].split("LatLng")[-1].split(")")[0].split(",")[1].replace("(","")
            item_loader.add_value("longitude",longitude)
        elevator=response.xpath("//h4[.='Caratteristiche Esterne']/following-sibling::ul//li/span[.='Ascensore']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        balcony=response.xpath("//h4[.='Caratteristiche Esterne']/following-sibling::ul//li/span[.='Balcone/i']").get()
        if balcony:
            item_loader.add_value("balcony",True)
        terrace=response.xpath("//h4[.='Caratteristiche Esterne']/following-sibling::ul//li/span[.='Terrazzo/i']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        item_loader.add_value("landlord_name","Immobiliare Franco")

        item_loader.add_value("landlord_email","info@immobiliarefranco.it")
        item_loader.add_value("landlord_phone","+39055 412910")

        room_count = response.xpath("//span[@title='Locali']/following-sibling::b/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        


        yield item_loader.load_item()