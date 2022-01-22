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
import dateparser

class MySpider(Spider):
    name = 'syushousing_be'
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    external_source = 'Syushousing_PySpider_belgium'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://tehuur.syushousing.be/te-huur?searchon=list&genres=Flat_Unspecified%2CGroundfloorFlat%2CRoofAppartement%2CDuplex%2CFlat_Building%2CLoft%2CPenthouse%2CTriplex%2CFlat_Other",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://tehuur.syushousing.be/te-huur?searchon=list&genres=Studio",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://tehuur.syushousing.be/te-huur?searchon=list&genres=Room%2CRoom_Other%2CStudentRoom",
                ],
                "property_type": "room"
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
        
        for item in response.xpath("//div[@data-view='showOnList']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'paging__item--next ')]/@href").get()
        if next_page:
            base_url = "https://tehuur.syushousing.be"
            next_page = base_url + next_page
            yield Request(
                next_page,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", self.external_source)

        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("/")[-1])

        title = response.xpath("//div[@class='row tab description']//h3/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        rent = response.xpath("//h1//text()[contains(.,'Huurprij')]").get()
        if rent:
            rent = rent.split("€")[1].split("/")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        address = " ".join(response.xpath("//td[contains(.,'Adres')]/following-sibling::td/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(" ")[-1]
            if city:
                item_loader.add_value("city", city.strip())
            zipcode = address.split(" ")[1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())     
        
        if property_type == "studio":
            item_loader.add_value("room_count", 1)
        else:
            room_count = response.xpath("//td[contains(.,'Slaapkamer')]/following-sibling::td/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//td[contains(.,'Badkamer')]/following-sibling::td/text()").get()
        if bathroom_count and "ja" in bathroom_count.lower():
            item_loader.add_value("bathroom_count", 1)
        else:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//td[contains(.,'Bewoonbare')]/following-sibling::td/text()").get()
        if square_meters:
           item_loader.add_value("square_meters", square_meters.split(" ")[0].strip())
        
        utilities = response.xpath("//td[contains(.,'Totale koste')]/following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].split("/")[0].strip()
            item_loader.add_value("utilities", utilities)

        floor = response.xpath("//td[contains(.,'Op verdie')]/following-sibling::td/text()").get()
        if floor:
            if int(floor) > 0:
                item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//td[contains(.,'Gemeubeld')]/following-sibling::td/text()").get()
        if furnished and "ja" in furnished.lower():
            item_loader.add_value("furnished", True)

        description = " ".join(response.xpath("//div[@class='row tab description']//p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            
        images = [x for x in response.xpath("//a[@class='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = "".join(response.xpath("//script/text()[contains(.,'enableMap')]").getall())
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude: ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('longitude: ')[1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Syus Housing")
        item_loader.add_value("landlord_phone", "+32 16 799 000")
        item_loader.add_value("landlord_email", "info@syushousing.be")
        
        yield item_loader.load_item()