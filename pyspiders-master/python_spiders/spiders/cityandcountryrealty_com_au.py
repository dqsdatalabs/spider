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
    name = 'cityandcountryrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'www.cityandcountryrealty.com.au',
        'accept': '*/*',
        'referer': 'https://www.cityandcountryrealty.com.au/rent?listing_cat=rental&category_ids=44&page=2',
        'accept-language': 'tr,en;q=0.9',
        'Cookie': 'PHPSESSID=nja08naad7mt47kpuhp3s0chi0'
    }

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.cityandcountryrealty.com.au/propertiesJson?callback=angular.callbacks._1&currentPage=1&perPage=12&sort=d_listing%20desc&listing_cat=rental&category_ids=45",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cityandcountryrealty.com.au/propertiesJson?callback=angular.callbacks._1&currentPage=1&perPage=12&sort=d_listing%20desc&listing_cat=rental&category_ids=44",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        data = json.loads(str(response.body).split("angular.callbacks._1(")[1].split(');')[0])
        max_page = data["paginationParams"]["totalPages"]

        for item in data["rows"]:
            follow_url = "https://www.cityandcountryrealty.com.au" + item["fields"]["link"].replace("\\", "")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= int(max_page): 
            follow_url = response.url.replace("currentPage=" + str(page - 1), "currentPage=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cityandcountryrealty_Com_PySpider_australia")          
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_xpath("room_count", "//span[i[@class='icon-realestate-bedrooms']]/span/text()")
        item_loader.add_xpath("bathroom_count", "//span[i[@class='icon-realestate-bathrooms']]/span/text()")
        item_loader.add_xpath("external_id", "//tr[th[.='Property ID']]/td/text()")        
        rent = response.xpath("//tr[th[.='Price']]/td/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().replace(',', '').split(" ")[0]
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
        
        city = response.xpath("//tr[th[.='Address']]/td/text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip())
       
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td/text()")
        parking = response.xpath("//span[i[@class='icon-realestate-garages']]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
  
        available_date = response.xpath("//tr[th[.='Available']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1].replace("!","").strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='property-details-section']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        
        script_map = response.xpath("//script[contains(.,'L.marker([')][1]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        images = [x for x in response.xpath("//div[contains(@class,'slideshow-enlarge')]/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_value("landlord_name", " City and Country Realty")
        item_loader.add_value("landlord_phone", "07 4743 9499")
        item_loader.add_value("landlord_email", "office@cityandcountryrealty.com.au")

        yield item_loader.load_item()