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
    name = 'empirechase_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.zoopla.co.uk/to-rent/branch/empire-chase-harrow-53315/?branch_id=53315&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine&page_size=50", "property_type": "apartment"},
	        {"url": "https://www.zoopla.co.uk/to-rent/branch/empire-chase-harrow-53315/?branch_id=53315&include_shared_accommodation=false&page_size=50&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine", "property_type": "house"},
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

        rented = "".join(response.xpath("//li[@class='ui-property-indicators__item']/span/text()").extract())
        if "let" in rented.lower():
            return

        if "Studio" in response.xpath("//span[@data-testid='title-label']/text()").get():
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_xpath("title", "//title//text()")
        item_loader.add_value("external_source", "Empirechase_Co_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("external_link", response.url)

        rent = response.xpath("//span[@data-testid ='price']/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("pcm")[0].replace(",","."))
            item_loader.add_value("currency", "GBP")

        room = response.xpath("//span[@data-testid='beds-label']/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.split(" ")[0])
        

        bathroom_count = response.xpath("//span[@data-testid='baths-label']/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        address = " ".join(response.xpath("//span[@data-testid='address-label']/text()").extract())
        if address:           
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0].split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        # available_date=response.xpath("//ul/li[@class='dp-features-list__item']/span[contains(.,'Available from')]/text()").get()
        # if available_date:
        #     date2 =  available_date.split("from")[1].strip()
        #     date_parsed = dateparser.parse(
        #         date2, date_formats=["%m-%d-%Y"]
        #     )
        #     date3 = date_parsed.strftime("%Y-%m-%d")
        #     item_loader.add_value("available_date", date3)

        desc = "".join(response.xpath("//div[@class='css-5vkfae-RichText e13tdjjp0']/span//text()").getall())
        if desc:
            item_loader.add_value("description", desc)

        label = "".join(response.xpath("//div[@class='css-5vkfae-RichText e13tdjjp0']/span//text()[contains(.,'EPC')]").getall())
        if label:
            item_loader.add_value("energy_label", label.split("Band")[1].replace(".",""))

        latlong = " ".join(response.xpath("//script [@type='application/ld+json']/text()[contains(.,'geo')]").extract())
        if latlong:
            lat = " ".join(response.xpath("substring-before(substring-after(//script [@type='application/ld+json']/text()[contains(.,'geo')],'latitude'),',')").extract())
            lng = " ".join(response.xpath("substring-before(substring-after(//script [@type='application/ld+json']/text()[contains(.,'geo')],'longitude'),'}')").extract())
            item_loader.add_value("latitude",lat.replace('":',"").strip())
            item_loader.add_value("longitude",lng.replace('":',"").strip() )


        img_json =  " ".join(response.xpath("//script [@type='application/ld+json']/text()[contains(.,'geo')]").extract())
        jseb = json.loads(img_json)
        images = [x["contentUrl"]  for x in jseb["@graph"][3]["photo"]]
        if images:
            item_loader.add_value("images", images)

        # water_cost = "".join(response.xpath("substring-before(substring-after(//a[span[contains(.,'Water')]]/span[2]/text(),'from'),'p/m')").getall())
        # if water_cost:
        #     item_loader.add_value("water_cost", water_cost.strip())

        furnished = " ".join(response.xpath("//section[@data-testid='page_features_section']/div/ul/li[contains(.,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True) 
        else:
            unfurnished = " ".join(response.xpath("//section[@data-testid='page_features_section']/div/ul/li[contains(.,'Unfurnished')]/text()").extract())
            if unfurnished:
                item_loader.add_value("furnished", False)


        terrace = " ".join(response.xpath("//section[@data-testid='page_features_section']/div/ul/li[contains(.,'Terrace')]/text()").extract())
        if terrace:
            item_loader.add_value("terrace", True) 

        balcony = " ".join(response.xpath("//section[@data-testid='page_features_section']/div/ul/li[contains(.,'Balcony')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        parking = " ".join(response.xpath("//section[@data-testid='page_features_section']/div/ul/li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 

        item_loader.add_value("landlord_name", "Empire Chase")
        item_loader.add_value("landlord_phone", "020 3478 2987")

        yield item_loader.load_item()