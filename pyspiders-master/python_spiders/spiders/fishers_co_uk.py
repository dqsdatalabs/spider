# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from scrapy import Request,FormRequest
import re 

class MySpider(Spider):
    name = 'fishers_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.fishers.co.uk/property/?wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_radius=10&wppf_records=12",
                    "http://www.fishers.co.uk/SearchResults.aspx?SearchData=v7|400|0|100000|0|99999998|0|0|3|3|0|1|1|All||2|11|0|2|1||1||||468|0:2147483647:0|4421|2|1|::"
                ],
                "property_type": "apartment"
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
        
        for item in response.xpath("//div[@class='property-cta']"):
            url = item.xpath("./a/@href").get()
            let_agreed = item.xpath(".//a[contains(.,'Let Agreed')]").get()
            if let_agreed:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        next_page = response.xpath("//div[@class='nav-links']/a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'?p=')")
        # item_loader.add_value("external_id", response.url.split("propid=")[1].split("&")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Fishers_Co_PySpider_united_kingdom")

        title = " ".join(response.xpath("//div[@class='property-details-container prop-single-eq']/div/div[@class='property-title']/text()").getall())
        if title:
            address = title.split("-")[0].strip()
            city = address.split(",")[-1].strip()
            if city == "":
                city = address.split(",")[-2]
            
            item_loader.add_value("title", title)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        zipcode=response.xpath("//script[contains(.,'name')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("streetAddress")[-1].split(",")[-1].replace("}","").replace('"',"").split("\\")[0])

        room_count = " ".join(response.xpath("substring-before(//div[@class='content']/text()[contains(.,'Bed')],'Bed')").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(@id,'property_info')]//p[contains(.,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("bath")[0].split(",")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
            if bathroom_count and bathroom_count.split(" ")[0]:
                if "two" in bathroom_count.lower(): item_loader.add_value("bathroom_count", "2")
                else: item_loader.add_value("bathroom_count", "1")
            
        rent = response.xpath("//div[@class='property-details-container prop-single-eq']/div/div[@class='property-title']/text()").get()
        if rent:
            rent = rent.split("£")[1].split("pm")[0].strip().replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")


        desc = " ".join(response.xpath("//div[@class='property-details']/div//p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//a[@class='property-image']/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        location=response.xpath("//iframe[contains(@src,'maps?')]/@src").get()
        if location:
            item_loader.add_value("longitude",location.split("maps?q")[-1].split(",")[0].replace("=",""))
            item_loader.add_value("latitude",location.split("maps?q")[-1].split("&")[0].split(",")[-1])
        

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = " ".join(response.xpath("//div[@class='property-details']/div/ul/li//text()").getall())
        if furnished:

            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
            if "parking" in furnished.lower():
                item_loader.add_value("furnished", True)

        floor = response.xpath("//div[@class='property-details']/div/ul/li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//li[contains(@id,'streetView')]//@href").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat=')[1].split('&')[0]
            longitude = latitude_longitude.split('lng=')[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Fishers Lettings")
        item_loader.add_value("landlord_phone", "0121 428 1000")
        item_loader.add_value("landlord_email","info@fishers.co.uk")

        yield item_loader.load_item()