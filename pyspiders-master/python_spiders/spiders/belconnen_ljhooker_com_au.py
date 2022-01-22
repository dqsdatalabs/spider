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
    name = 'belconnen_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://belconnen.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=true",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://belconnen.ljhooker.com.au/search/house+townhouse+duplex_semi_detached+penthouse+terrace-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://belconnen.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-content')]/div[@onclick]//a[contains(@class,'property-sticker')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        night = response.xpath("//div[@class='property-heading']/h2/text()").extract_first()
        if "night" in night.lower():
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Belconnen_Ljhooker_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("address", "//div[@class='property-heading']/h1/text()")
        item_loader.add_xpath("zipcode", "substring-after(//div[@class='property-heading']/h1/text(),', ')")
        item_loader.add_xpath("room_count", "//span[@class='bed ']/text()")
        item_loader.add_xpath("bathroom_count", "//span[@class='bathroom ']/text()")


        city = response.xpath("substring-before(//div[@class='property-heading']/h1/text(),',')").extract_first()
        if city:
            item_loader.add_value("city", city.split(" ")[-1].strip())

        zipcode = response.xpath("substring-before(substring-after(//script[contains(text(),'var dataLayer = ')],'postcode'),',')").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.replace('"','').replace(':','').strip())

        rent = "".join(response.xpath("//div[@class='property-heading']/h2/text()").extract())
        if rent:
            if "week" in rent.lower():
                price = rent.split(" ")[0].replace("\xa0",".").replace(",",".").replace(" ","").replace("$","").strip()
                if price !="NC":
                    item_loader.add_value("rent", int(float(price))*4)
                    item_loader.add_value("currency", "AUD")

        latitude = " ".join(response.xpath("substring-before(substring-after(//script[@type='application/ld+json']/text(),'latitude'),',')").extract())
        if latitude:
            lat = latitude.replace('":"',"").replace('"',"")
            item_loader.add_value("latitude", lat.strip())

        longitude = " ".join(response.xpath("substring-before(substring-after(//script[@type='application/ld+json']/text(),'longitude'),'}')").extract())
        if longitude:
            lng = longitude.replace('":"',"").replace('"',"")
            item_loader.add_value("longitude", lng.strip())

        external_id = " ".join(response.xpath("substring-after(//div[@class='code']/text(),'  ')").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        available_date=response.xpath("//li/strong[.='Date Available:']/following-sibling::text()").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

      
        desc = " ".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='slidethumb']/div//img/@src[not(contains(.,'data'))]").extract()]
        if images:
                item_loader.add_value("images", images)


        parking =response.xpath("//span[@class='carpot ']/text()").extract_first()    
        if parking:
            (item_loader.add_value("parking", True) if parking !="0" else item_loader.add_value("parking", False))

        dishwasher ="".join(response.xpath("//div[@class='col-md-7']/ul/li/text()[contains(.,'Dishwasher')]").extract())   
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        pets_allowed ="".join(response.xpath("//li/strong[.='Pets Allowed:']/following-sibling::text()").extract())   
        if pets_allowed:
            if "yes" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)
            elif "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)

        furnished ="".join(response.xpath("//li/strong[.='Furniture:']/following-sibling::text()").extract())   
        if furnished:
            if "yes" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "no" in furnished.lower():
                item_loader.add_value("furnished", False)


        item_loader.add_xpath("landlord_phone", "//span[@class='agent-mobile']/strong//text()")

        landlord_name = response.xpath("//div[@class='inspection-address']/p/a/text()[.!='Email Office']").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())  

        item_loader.add_value("landlord_email", "leasingconsultant.belconnen@ljh.com.au")               

        yield item_loader.load_item()