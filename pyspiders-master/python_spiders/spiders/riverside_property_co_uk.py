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
    name = 'riverside_property_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=221&max=0&location=&search=&page={}&up=true",
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=225&max=0&location=&search=&page={}&up=true"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=226&max=0&location=&search=&page={}&up=true",
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=228&max=0&location=&search=&page={}&up=true",
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=224&max=0&location=&search=&page={}&up=true",
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=222&max=0&location=&search=&page={}&up=true"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=763&max=0&location=&search=&page={}&up=true",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "http://riverside-property.co.uk/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type=1884&max=0&location=&search=&page={}&up=true",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='searchResultPhoto']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Riverside_Property_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div/h1/text()")
        address = response.xpath("//div/h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            if len(address.split(","))>2:
                item_loader.add_value("zipcode", address.split(",")[-1].strip())
                item_loader.add_value("city", address.split(",")[-2].strip())
            
        item_loader.add_value("external_id", response.url.split("propertyID=")[1].strip())
       
        rent = response.xpath("//div[@id='particularsLeftPanel']/h2/span/text()[.!='POA  pcm ']").get()
        if rent:
            rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", rent)     
        else:
            rent = response.xpath("//div[@id='particularsLeftPanel']/p//text()[contains(.,'Rent:')]").get()
            if rent:
                rent = rent.lower().split('£')[-1].strip().split(' ')[0].replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent) 

        deposit = "".join(response.xpath("//div[contains(@id,'particularsLeftPanel')]//p[contains(.,'Bond')]//text()").getall())
        if deposit:
            deposit = deposit.replace(":","").split("Bond")[1].replace("A","").split("£")[1].strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)
        else:
            deposit = "".join(response.xpath("//div[contains(@id,'particularsLeftPanel')]//p[contains(.,'Deposit')]//text()").getall())
            if deposit:
                deposit = deposit.split("Deposit")[1].split("£")[1].replace("A","").strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)
        
        item_loader.add_value("currency", "GBP")
    
        available_date = response.xpath("//ul//li[contains(.,'Available ')]/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split("available")[1].replace("from","").replace("after","").replace("end","").replace("beginning","").replace("of","").strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        room_count = response.xpath("//div[@id='particularsLeftPanel']/h2[contains(.,'Bed')]/text()").get()
        if room_count:        
            room_count = room_count.split("Bed")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count",room_count)
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count","1")

        from word2number import w2n
        bathroom_count = response.xpath("//ul//li[contains(.,'Bathroom')]/span/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0].strip()
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except : pass
  
        script_map = response.xpath("//script[contains(.,'position: new google.maps.LatLng(')]//text()").get()
        if script_map:
            latlng = script_map.split("position: new google.maps.LatLng(")[1].split(")")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        parking = response.xpath("//ul//li[contains(.,'parking') or contains(.,'Parking')]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//ul//li[contains(.,'Terrace')]/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
 
        floor = response.xpath("substring-before(//ul//li[contains(.,'Floor') and not(contains(.,'All')) and not(contains(.,'Apartment'))]/span/text(),'Floor')").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        energy_label = response.xpath("substring-after(//ul//li[contains(.,'EPC Rating')]/span/text(),'EPC Rating')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.replace("-","").strip())
        furnished = response.xpath("//ul//li[contains(.,'Furnished') or contains(.,'furnished')]/span/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        desc = " ".join(response.xpath("//div[@id='particularsLeftPanel']/h2/following-sibling::p[1]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())         
                    
        images = [ response.urljoin(x) for x in response.xpath("//div[@id='galleria']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Riverside Property")
        item_loader.add_value("landlord_phone","01482 322411")    
        item_loader.add_value("landlord_email","info@riverside-property.co.uk")    
        yield item_loader.load_item()