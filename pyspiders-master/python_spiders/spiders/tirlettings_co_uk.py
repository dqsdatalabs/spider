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
    name = 'tirlettings_co_uk'
    execution_type = 'testing' 
    country = 'united_kingdom'
    locale ='en'
    #https://www.tirlettings.co.uk/home/properties_to_let/
    def start_requests(self):
        start_urls = [
            {
                "url": ["https://www.rightmove.co.uk/property-to-rent/find/TIR-Lettings-Ltd/Sheffield.html?locationIdentifier=BRANCH%5E73976&propertyStatus=all&includeLetAgreed=true&_includeLetAgreed=on"
                ],
                "property_type": "apartment"
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={
                        'property_type': url.get('property_type'),
                    }
                )

    # 1. FOLLOWING 
    def parse(self, response):

        property_type =  response.meta.get('property_type')
        for item in response.xpath("//a[@class='propertyCard-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type": property_type})



    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "TirLettings_PySpider_united_kingdom")
        proptype =response.xpath("//div[.='PROPERTY TYPE']/parent::div/following-sibling::div/div[2]/div/text()").get()

        if get_p_type_string(proptype):
            item_loader.add_value("property_type", get_p_type_string(proptype))
        else:
            return  

        item_loader.add_value("external_id", response.url.split("/")[-1])

        title ="".join(response.xpath("//h1//text()").get())
        if title:
            title=title.replace("\n","").replace("\t","")
            title=re.sub('\s{2,}',' ',title.strip())
            item_loader.add_value("title", title)
        desc = "".join(response.xpath("//h2[.='Property description']/following-sibling::div/div//text()").get())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        
        address =response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(",")[-2]
            item_loader.add_value("city", city.strip())
            zipcode=address.split(",")[-1]
            item_loader.add_value("zipcode", zipcode.strip())
        rent =response.xpath("//div[contains(.,'pw')]/text()").get()
        if rent:
            rent = rent.strip().split("£")[1].split("pw")[0].strip()
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")
        deposit =response.xpath("//dt[.='Deposit: ']/following-sibling::dd//text()").getall()
        if deposit:
            deposit = deposit[1].strip()
            item_loader.add_value("deposit", int(deposit))

        room_count =response.xpath("//div[.='BEDROOMS']/parent::div/following-sibling::div/div[2]/div/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)

        bathroom_count =response.xpath("//div[.='BATHROOMS']/parent::div/following-sibling::div/div[2]/div/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        furnished = response.xpath("//dt[.='Furnish type: ']/following-sibling::dd//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        images = [x for x in response.xpath("//meta[@itemprop='contentUrl']//@content").getall()]
        if images:
            item_loader.add_value("images", images)
        location=response.xpath("//script[contains(.,'longitude')]/text()").get()
        if location:
            latitude=location.split("location':{")[-1].split("latitude")[-1].split(",")[0].replace('\":',"")
            item_loader.add_value("latitude",latitude)
            longitude=location.split("location':{")[-1].split("longitude")[-1].split(",")[0].replace('\":',"")
            item_loader.add_value("longitude",longitude)


        from datetime import datetime
        import dateparser
        available_date = response.xpath("//dt[.='Let available date: ']/following-sibling::dd/text()").get()
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        item_loader.add_value("landlord_name", "TIR Lettings Ltd, Sheffield")
        item_loader.add_value("landlord_email", "customersupport@rightmove.co.uk")
        item_loader.add_value("landlord_phone", "01274399017")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
