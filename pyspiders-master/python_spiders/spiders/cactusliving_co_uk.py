# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
from scrapy import Request,FormRequest


class CactuslivingCoUkSpider(scrapy.Spider):
    name = "cactusliving_co_uk"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    external_source='Cactusliving_PySpider_united_kingdom_en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.linleyandsimpson.co.uk/gallery-view.php?transType=2&hideunavailable=1&propType=Flat/Apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.linleyandsimpson.co.uk/list-view.php?transType=2&FormSearchText=&priceMin=&priceMax=&propType=House&beds=&rentFurnishing=0&radius=&hideunavailable=1&action_ProcessSend=Search",
                ],
                "property_type" : "house"
            },
        ]

        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):
    
        for item in response.xpath("//a[@class='btn btn-secondary']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})
     
        next_page = response.xpath("//span[contains(.,'Next')]/parent::a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )
                        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        external_id = "".join(response.url)
        if external_id:
            if "=_" in external_id:
                external_id = external_id.split("=_")[1]
            else:
                external_id = external_id.split("=")[1]
            item_loader.add_value('external_id', external_id)
  
        title = response.xpath('//title//text()').extract()
        if title:
            item_loader.add_value('title', title)
     
        room_count = response.xpath('(//p[@class="bedrooms"]//span/text())[1]').get()
        if room_count:
            room_count = room_count.split(" Bed")[0]
            item_loader.add_value('room_count', room_count)

        description = response.xpath('//div[@class="twoColumns"]//text()').extract()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
            item_loader.add_value('description',description)

        rent = response.xpath('//h2[@id="price"]//text()').get()
        if rent:
            rent = rent.strip()
            rent = rent.split("£")[1]
            if "pcm" in rent.lower():
               rent = rent.split("pcm")[0]
            if "," in rent:
                rent = rent.replace(",","")
            item_loader.add_value('rent', rent)
        item_loader.add_value("currency", "GBP")

        images = response.xpath('//div[contains(@class,"lazy slider")]//img/@data-src').extract()
        if images:
            images = list(set(images))
            item_loader.add_value('images', images)

        item_loader.add_value('landlord_name', "Linley & Simpson")
        item_loader.add_value('landlord_phone', '0113 237 0160')
        
        yield item_loader.load_item()
