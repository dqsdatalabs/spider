# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'londonwidelettings_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Londonwidelettings_Co_PySpider_united_kingdom'
    custom_settings={
        "HTTPCACHE_ENABLE":False,
        "PROXY_UK_ON" : True
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.zoopla.co.uk/to-rent/branch/london-wide-lettings-hampstead-33704/?branch_id=33704&include_rented=true&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine", "property_type": "apartment"},
	        {"url": "https://www.zoopla.co.uk/to-rent/branch/london-wide-lettings-hampstead-33704/?branch_id=33704&include_rented=true&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine", "property_type": "house"},
            {"url":"https://www.zoopla.co.uk/to-rent/property/london/nw3/hampstead-belsize-park-swiss-cottage/?price_frequency=per_month&q=NW3&results_sort=newest_listings&search_source=to-rent"}
        ]  # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        prop_type = response.meta.get("property_type") 

        for item in response.xpath("//div[@data-testid='search-result']/div//div[2]//a[@data-testid='listing-details-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop_type})

        pagination = response.xpath("//div[@data-testid='pagination']//a[.='Next >']/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse,meta={"property_type":prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        title = response.xpath("//title//text()").get()

        rented = "".join(response.xpath("//li[@class='ui-property-indicators__item']/span/text()").extract())
        if "let" in rented.lower():
            return
        if "rent" not in response.url:
            return
        if response.meta.get('property_type'):
            item_loader.add_value("property_type", response.meta.get('property_type'))
        else:
            desc_text = " ".join(response.xpath("//div[@data-testid='truncated_text_container']/div/span/text()").getall())
            if title and get_p_type_string(title):
                item_loader.add_value("property_type",get_p_type_string(title))
            elif desc_text and get_p_type_string(desc_text):
                item_loader.add_value("property_type",get_p_type_string(desc_text))
            else:
                return
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_link", response.url)
        
        item_loader.add_xpath("title", "//title//text()")

        rent = response.xpath("//span[@data-testid='price']//text()[.!='POA']").extract_first()
        if rent:
            rent=rent.split("pcm")[0].split("£")[1].replace(",","")
            item_loader.add_value("rent", rent.split("pcm")[0])
        item_loader.add_value("currency", "GBP")
        address=response.xpath("//span[@data-testid='address-label']/text()").get()
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//span[@data-testid='address-label']/text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip().split(" ")[0])
        zipcode=response.xpath("//span[@data-testid='address-label']/text()").get()
        if zipcode:
            zipcode=re.search("[A-Z][A-Z].*",zipcode)
            item_loader.add_value("zipcode",zipcode.group())

        bathroom_count = response.xpath("//span[@data-testid='baths-label']//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        room_count = response.xpath("//span[@data-testid='beds-label']//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])

        desc = " ".join(response.xpath("//div[@data-testid='listing_description']//span//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
            deposit = re.search("Deposit /[ /\w:]+£([ \d,]+)", desc)
            if deposit:
                deposit = deposit.group(1).replace(",","")
                item_loader.add_value("deposit",deposit)

        latitude_longitude = response.xpath("//script[contains(.,'GeoCoordinates')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude": ')[1].split(',')[0]
            longitude = latitude_longitude.split('"longitude":')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        furnished=response.xpath("//li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        
        available_date="".join(response.xpath("//span[@data-testid='availability']//text()").getall())
        if available_date:
            available_date=available_date.split("Available from")[-1].strip() 
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [
            response.urljoin(x)
            for x in response.xpath("//li[@data-testid='gallery-image']//img[@role='img']//@src").extract()
        ]
        item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "London Wide Lettings")
        item_loader.add_value("landlord_phone", "020 3463 2515")

        external_id = (response.url).strip("/").split("/")[-1]
        item_loader.add_value("external_id",external_id)
        item_loader.add_value("city","London")
        yield item_loader.load_item()



def get_p_type_string(p_type_string):
    if ("appartement" in p_type_string.lower()):
        return "apartment"    
    elif "student" in p_type_string.lower():
        return "student_apartment"
    elif  ("studio" in p_type_string.lower()):
        return "studio"
    elif  ("property" in p_type_string.lower()):
        return "apartment"
    elif  ("bungalow" in p_type_string.lower()):
        return "house"
    elif  ("maisonette" in p_type_string.lower()):
        return "house"
    elif  ("cottage" in p_type_string.lower()):
        return "house"
    elif ("house" in p_type_string.lower()):
        return "house"
    elif ("flat" in p_type_string.lower()):
        return "apartment"
    elif ("villa" in p_type_string.lower()):
        return "house"
    elif ("room" in p_type_string.lower()):
        return "room"   
    elif "local" in p_type_string.lower():
        return None        
    else:
        return "apartment"