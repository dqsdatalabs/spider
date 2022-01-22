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
    name = 'habellis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.habellis.fr/a-louer/nos-logements-a-louer/?widget_id=4&kind=0&sf_radiussearchradius=10&sf_radiussearchunit=13&sf_multiple_property_type=6%2C&sf_unit_price=150&sf_min_price=0&sf_max_price=1000&sf_select_listing=13&sf_select_field_3027=1&wplpage=1",
                "property_type": "apartment"
            },
	        {
                "url": "https://www.habellis.fr/a-louer/nos-logements-a-louer/?widget_id=4&kind=0&sf_radiussearchradius=10&sf_radiussearchunit=13&sf_multiple_property_type=7%2C&sf_unit_price=150&sf_min_price=0&sf_max_price=1000&sf_select_listing=13&sf_select_field_3027=1&wplpage=1",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[contains(@id,'prp_link_id')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page={page-1}",f"page={page}")
            yield Request(
                url,
                callback=self.parse,
                meta={"page": page+1, "property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Habellis_PySpider_france")
                
        external_id = response.xpath("//h2[@class='location_build_up']//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[0])

        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        address = response.xpath("//h2/span/text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            item_loader.add_xpath("address", "//span[contains(@class,'programmeVilleBien')]/text()")
            item_loader.add_xpath("city", "//span[contains(@class,'programmeVilleBien')]/text()")
        
        city = response.xpath("//label[contains(.,'Ville')]/following-sibling::span/text()").get()
        item_loader.add_value("city", city)

        zipcode = response.xpath("//label[contains(.,'Code')]/following-sibling::span/text()").get()
        item_loader.add_value("zipcode", zipcode)
         
        room_type = response.xpath("//span/span/text()").get()
        room_count = response.xpath("//div[@class='wpl_prp_show_detail_boxes_title']//span[contains(.,'pièces')]//text()").get()
        if room_count:
            room_count="".join(room_count.split(" ")[1:2])
            item_loader.add_value("room_count", room_count)
        elif "studio" in room_type.lower():
            item_loader.add_value("room_count", "1")
        elif not room_count:
            room1=response.xpath("//span[contains(.,'Appartement')]/text()").get()
            if room1:
                room1=re.findall("\d+",room1)
                item_loader.add_value("room_count",room1)



        square_meters = response.xpath("//div[@class='built_up_area']//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))
        
        rent = response.xpath("//li[contains(.,'Total')]/span/text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(",",".")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//li[contains(.,'Charge')]/span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        description = " ".join(response.xpath("//div[@id='et-boc']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//li[contains(@id,'wpl-gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'googlemap_lt\":')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('map_lt":')[1].split(',')[0]
            longitude = latitude_longitude.split('map_ln":')[1].split(',')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        balcony = response.xpath("//label[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony and "oui" in balcony.lower():
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//label[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//label[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        external_id = response.xpath("//p/text()[contains(.,'Réf')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip().replace(".",""))

        item_loader.add_value("landlord_name", "HABELLIS")
        item_loader.add_value("landlord_phone", " 03 80 68 28 00")
        item_loader.add_value("landlord_email", "cindy.blanchard@habellis.fr")
        
        yield item_loader.load_item()