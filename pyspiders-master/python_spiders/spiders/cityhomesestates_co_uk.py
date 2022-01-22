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

class MySpider(Spider):
    name = 'cityhomesestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=apartment&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=first-floor-flat&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=flat&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=first-floor-studio&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "studio"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=ground-floor-flat&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=top-floor-flat&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=third-floor-flat&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "apartment"},
            {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=studio&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "studio"},
	        {"url": "https://cityhomesestates.co.uk/advanced-search/?adv_location=&filter_search_action%5B%5D=to-let&is10=10&filter_search_type%5B%5D=house&bedrooms=&bathrooms=&price_low=0&price_max=2000000", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for follow_url in response.xpath("//div[@id='listing_ajax_container']/div[contains(@class,'listing_wrapper')]//h4/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul[contains(@class,'pagination')]/li[@class='roundright']/a/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Cityhomesestates_Co_PySpider_"+ self.country)
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        if "Studio" in title:
            item_loader.add_value("room_count", "1")
        elif "Bed" in title:
            room_count = title.split("Bed")[0].strip()
            if room_count != "0":
                item_loader.add_value("room_count", room_count)
        
        rent = "".join(response.xpath(
            "//div[contains(@class,'detail')]//span[contains(@class,'price_label')]/parent::span//text()"
            ).getall())
        if rent:
            price = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            if "." in price:
                item_loader.add_value("rent", price.split(".")[0])
            else:
                item_loader.add_value("rent", price)
        
        item_loader.add_value("currency", "GBP")
        
        address = "".join(response.xpath("//div/strong[contains(.,'Address')]/parent::div//text()").getall())
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
        
        city = "".join(response.xpath("//div/strong[contains(.,'City')]/parent::div//text()").getall())
        if city:
            item_loader.add_value("city", city.split(":")[1].strip())
        
        zipcode = "".join(response.xpath("//div/strong[contains(.,'Zip')]/parent::div//text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())
        
        external_id = "".join(response.xpath("//div/strong[contains(.,'Property Id')]/parent::div//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        available_date = "".join(response.xpath("//div/strong[contains(.,'Available')]/parent::div//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude = response.xpath("//div/@data-cur_lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div/@data-cur_long").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
            
        desc = "".join(response.xpath("//div[contains(@class,'property_details')]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images = [ x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        # if "parking" in desc.lower():
        #     item_loader.add_value("parking", True)
        
        # if "lift" in desc.lower():
        #     item_loader.add_value("elevator", True)
        
        # if "terrace" in desc.lower():
        #     item_loader.add_value("terrace", True)
        
        # if "pool" in desc.lower():
        #     item_loader.add_value("swimming_pool", True)
            
        # if "unfurnished" in desc.lower():
        #     item_loader.add_value("furnished", False)
        # elif "furnished" in desc.lower():
        #     item_loader.add_value("furnished", True)

        # if "washing machine" in desc.lower():
        #     item_loader.add_value("washing_machine", True)
        
        # if "dishwasher" in desc.lower():
        #     item_loader.add_value("dishwasher", True)

        # if "balcon" in desc.lower():
        #     item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_name","Cityhomes Estates")
        item_loader.add_value("landlord_phone","020 3475 3474")
        item_loader.add_value("landlord_email","info@cityhomesestates.co.uk")
            
            
            
        yield item_loader.load_item()