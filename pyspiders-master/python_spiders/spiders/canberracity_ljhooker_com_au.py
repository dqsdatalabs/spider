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
    name = 'canberracity_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://canberracity.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=False&liveability=False",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://canberracity.ljhooker.com.au/search/house-for-rent/page-1?surrounding=False&liveability=False",
                    "https://canberracity.ljhooker.com.au/search/townhouse-for-rent/page-1?surrounding=False&liveability=False",
                    "https://canberracity.ljhooker.com.au/search/duplex_semi_detached-for-rent/page-1?surrounding=False&liveability=False",
                    "https://canberracity.ljhooker.com.au/search/penthouse-for-rent/page-1?surrounding=False&liveability=False",
                    "https://canberracity.ljhooker.com.au/search/terrace-for-rent/page-1?surrounding=False&liveability=False"
                    
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://canberracity.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=False&liveability=False",
                ],
                "property_type": "studio"
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
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h3/a/@href").extract():
            yield Request(item, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Canberracity_Ljhooker_PySpider_australia")
        item_loader.add_xpath("title", "//div[@class='property-heading']/h1/text()")

        rent = "".join(response.xpath("//h2//text()").getall())
        if rent:
            rent = ''.join(c for c in rent.split("$")[1].strip().split(" ")[0] if c.isdigit())
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        
        item_loader.add_xpath("room_count","//div[@class='property-icon flat-theme']/span[@class='bed ']/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='property-icon flat-theme']/span[@class='bathroom ']//text()")

        city = response.xpath("//script[@type='text/javascript']/text()[contains(.,'suburb') and contains(.,'dataLayer ')]").get()
        if city:
            item_loader.add_value("city", city.split('"suburb": "')[1].split('"')[0])
        zipcode = response.xpath("//script[@type='text/javascript']/text()[contains(.,'postcode') and contains(.,'dataLayer ')]").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('"postcode": "')[1].split('"')[0])
        address = " ".join(response.xpath("//div[@class='property-heading']/h1//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
           
        item_loader.add_xpath("latitude","//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@property='og:longitude']/@content")

        desc =  " ".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        external_id =  " ".join(response.xpath("//div[@class='code']/text()").extract())
        if external_id:
            ext_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", ext_id)
  
        images = [ x for x in response.xpath("//div[@class='thumb']/span/img/@src[not(contains(.,'data:image/gif;base64,'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date="".join(response.xpath("//ul/li/strong[.='Date Available:']/following-sibling::text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        pets_allowed = response.xpath("//ul/li/strong[.='Pets Allowed:']/following-sibling::text()").get()
        if pets_allowed:
            if pets_allowed.lower().strip() == "no":
                item_loader.add_value("pets_allowed", False)
            elif pets_allowed.lower().strip() == "yes":
                item_loader.add_value("pets_allowed", True)

        parking = response.xpath("//div[@class='property-icon flat-theme']/span[@class='carpot ']//text()").extract_first()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        dishwasher = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Dishwasher')]/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        furnished = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").extract())
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'Elevator')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'balcony ')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)
        landlord_name = response.xpath("//script[contains(.,'telephone')]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.split('telephone":"')[0].split('name":"')[-1].split('"')[0]
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//script[contains(.,'email')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.split('email":"')[1].split('"')[0]
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//script[contains(.,'telephone')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('telephone":"')[1].split('"')[0]
            item_loader.add_value("landlord_phone", landlord_phone)
        yield item_loader.load_item()