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
    name = 'poswall_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Poswall_PySpider_united_kingdom_en"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.zoopla.co.uk/to-rent/branch/poswall-lettings-southampton-82646/?branch_id=82646&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine", "property_type": "apartment"},
	        {"url": "https://www.zoopla.co.uk/to-rent/branch/poswall-lettings-southampton-82646/?branch_id=82646&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        prop_type = response.meta.get("property_type")

        for item in response.xpath("//ul[contains(@class,'listing-results')]/li//a[contains(@class,'listing-results-price')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop_type})
        
        pagination = response.xpath("//div[contains(@class,'paginate')]/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse,meta={"property_type":prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "Studio" in response.xpath("//h1/span[1]/text()").get():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_link", response.url)
        json_data = response.xpath("//script[@type='application/ld+json']/text()").get()
        if json_data:
            data = json.loads(json_data)["@graph"]
            for item in data:
                if "@type" in item and item["@type"] == "Residence":
                    item_loader.add_value("description", item["description"])
                    address = item["address"]["streetAddress"]
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", item["address"]["addressLocality"])
                    item_loader.add_value("zipcode", address.split(" ")[-1])
                    item_loader.add_value("latitude", str(item["geo"]["latitude"]))
                    item_loader.add_value("longitude", str(item["geo"]["longitude"]))
                    
                    images = [x["contentUrl"] for x in item["photo"]]
                    item_loader.add_value("images", images)

        item_loader.add_xpath("title", "//h1/span[1]/text()")
        
        rent = response.xpath("//span[@data-testid='price']/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("pcm")[0].replace(",","."))
            item_loader.add_value("currency", "GBP")

        room = response.xpath("//span[@data-testid='beds-label']/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.split(" ")[0])

        bathroom_count = response.xpath("//span[@data-testid='baths-label']/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        available_date=response.xpath("//span[@data-testid='availability']/text()").get()
        if available_date:
            date2 =  available_date.split("from")[-1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        label = "".join(response.xpath("//div[@class='dp-description__text']/text()[contains(.,'EPC')]").getall())
        if label:
            item_loader.add_value("energy_label", label.split("Band")[1].replace(".",""))

        furnished = " ".join(response.xpath("//li[contains(.,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True) 
        
        parking = " ".join(response.xpath("//li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 
        balcony = " ".join(response.xpath("//li[contains(.,'Balcony')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True) 
        item_loader.add_value("landlord_name", "Poswall Lettings")
        item_loader.add_value("landlord_phone", "023 8234 9387")

        yield item_loader.load_item()