# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from python_spiders.helper import extract_number_only, remove_white_spaces



class MySpider(Spider):
    name = 'garypeer_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Garypeer_PySpider_australia"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.garypeer.com.au/rent/search-listings?search_q=&listing_type=Rent&house_type%5B%5D=Apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.garypeer.com.au/rent/search-listings?search_q=&listing_type=Rent&house_type%5B%5D=Townhouse&house_type%5B%5D=Unit&house_type%5B%5D=House&house_type%5B%5D=Villa",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.garypeer.com.au/rent/search-listings?search_q=&listing_type=Rent&house_type%5B%5D=Studio",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'col-lg-4 tile__item')]"):
            follow_url = response.urljoin(item.xpath(".//a[.='View listing']/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        pagination = response.xpath("//a[@class='j-load-next-page link-next-page']/@href").get()
        if pagination:
            yield Request(
                pagination,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count","//div[@class='listing-card']//li[@class='bedrooms']/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='listing-card']//li[@class='bathrooms']/text()")
        address = ", ".join(response.xpath("//div[@class='listing-card']/h1//text() | //div[@class='listing-card']/h3//text()").getall())
        if address:
            item_loader.add_value("address", remove_white_spaces(address))
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())

        rent = response.xpath("//div[@class='listing-details']//div[h4[.='Rent']]/p/text()").get()
        if rent:
            pw = extract_number_only(rent,thousand_separator=',',scale_separator='.')
            item_loader.add_value("rent", str(int(pw)*4))
        item_loader.add_value("currency", "AUD")

        description = " ".join(response.xpath("//div[@class='read-more__wrapper']//text()").getall())
        if description:
            item_loader.add_value("description",remove_white_spaces(description))

        images = [x for x in response.xpath("//div[@class='carousel j-listing-carousel']//figure[not(contains(@class,'floorplan'))]/@data-flickity-bg-lazyload").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='carousel j-listing-carousel']//figure[contains(@class,'floorplan')]/@data-flickity-bg-lazyload").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        deposit = response.xpath("//div[@class='read-more__wrapper']//text()[contains(.,'Bond:')]").get()
        if deposit:
            deposit = deposit.split('Bond:')[-1].strip().replace(' ', '')
            item_loader.add_value("deposit", deposit)

       
        parking = response.xpath("//th[contains(.,'Ascenseur')]/following-sibling::th/text()").get()
        if parking:
            if parking.strip().lower() == '0':
                parking = False
            else:
                parking = True
            item_loader.add_value("parking", parking)
        
        available_date = response.xpath("//div[@class='listing-details']//div[h4[.='Date available']]/p/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
     
        item_loader.add_xpath("landlord_name", "//div[@class='container -wide -persons']//div[@class='tile__item-content_inner']/a//text()[normalize-space()]")
        item_loader.add_xpath("landlord_phone", "//div[@class='container -wide -persons']//a[contains(@href,'tel:')][1]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='container -wide -persons']//a[contains(@href,'mailto')]/text()")
        yield item_loader.load_item()