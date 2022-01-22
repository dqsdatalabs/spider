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
    name = 'stmoores_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.rightmove.co.uk/api/_search?locationIdentifier=BRANCH%5E17339&numberOfPropertiesPerPage=24&radius=0.0&sortType=6&index=0&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false&propertyTypes=flat", "property_type": "apartment"},
	        {"url": "https://www.rightmove.co.uk/api/_search?locationIdentifier=BRANCH%5E17339&numberOfPropertiesPerPage=24&radius=0.0&sortType=6&index=0&propertyTypes=semi-detached%2Cterraced%2Cbungalow%2Cdetached&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 24)
        data = json.loads(response.body)

        if len(data["properties"]) > 0:
            for item in data["properties"]:
                follow_url = response.urljoin(item["propertyUrl"])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
                
            base_url = response.meta.get("base_url", response.url.replace("index=0","index=page_count"))
            url = base_url.replace("page_count",f"{str(page)}")
            yield Request(url, callback=self.parse, meta={"page": page+24, "base_url":base_url ,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_source", "Stmoores_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        rent = response.xpath("//div[@class='_1gfnqJ3Vtd1z40MlC0MzXu']/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))

        deposit = response.xpath("//div[@class='_2RnXSVJcWbWv4IpBC1Sng6'][contains(.,'Deposit')]/b/text()[2]").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",".").strip())

        room = response.xpath("//div[div[.='BEDROOMS']]/div[2]//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.replace("x",""))

        bathroom_count = response.xpath("//div[div[.='BATHROOMS']]/div[2]//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.replace("x",""))

        external_id = response.xpath("//b[contains(.,'Disclaimer')]/following-sibling::text()[contains(.,'Property reference')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('Property reference')[1].split('.')[0].strip())

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[contains(.,'Let available date')]/b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%d-%m-%Y")
                item_loader.add_value("available_date", date2)
        
        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        address = ""
        addres = " ".join(response.xpath("//h2[@class='gBaD1fLdPaNTbHErZvX7y']/text()").extract())
        if addres:    
            if "-" in addres:    
                address = addres.split("-")[1]
            else:
                address = addres
                item_loader.add_value("city", addres.split(",")[-2].strip()) 
                item_loader.add_value("zipcode", addres.split(",")[-1].strip())

            item_loader.add_value("address", address)   

        images = [
            response.urljoin(x)
            for x in response.xpath("//meta[@property ='og:image']/@content").extract()
        ]
        item_loader.add_value("images", images)

        desc = " ".join(response.xpath("//h2[contains(.,'Property description')]/following-sibling::div[1]//text()").getall()).strip()
        if desc:
            item_loader.add_value("description", desc.replace('\xa0', ''))

        label = "".join(response.xpath("//div[contains(@class,'kJR0bMoi8VLouNkBRKGww')]//text()[contains(.,'rating')]").getall())
        if desc:
            item_loader.add_value("description", label.strip().split("rating")[0])

        latlong = " ".join(response.xpath("//script[@type='text/javascript'][contains(.,'latitude')]/text()").extract())
        if latlong:
            lat = " ".join(response.xpath("substring-before(substring-after(//script[@type='text/javascript'][contains(.,'latitude')]/text(),'latitude'),',')").extract())
            lng = " ".join(response.xpath("substring-before(substring-after(//script[@type='text/javascript'][contains(.,'latitude')]/text(),'longitude'),',')").extract())
            item_loader.add_value("latitude",lat.replace('":',"").strip())
            item_loader.add_value("longitude",lng.replace('":',"").strip() )


        furnished = " ".join(response.xpath("//div[@class='_2RnXSVJcWbWv4IpBC1Sng6'][contains(.,'Furnish type')]/b/text()").extract())
        if furnished:
            if furnished == "Furnished":
                item_loader.add_value("furnished", True) 
            else:
                if furnished == "Unfurnished":
                    item_loader.add_value("furnished", False)

        parking = " ".join(response.xpath("//b[contains(.,'Parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 

        terrace = " ".join(response.xpath("//div[div[.='Terraced']]/div[2]//text()").extract())
        if terrace:
            item_loader.add_value("terrace", True) 

        item_loader.add_value("landlord_name", "St Moores Letting & Property Management Ltd")
        item_loader.add_value("landlord_phone", "02382 200448")
        

        yield item_loader.load_item()