# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'iamtheagent_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.iamtheagent.com/property-for-rent/search/?SearchType=rentals&Location=uk&Distance=40&MinSalePrice=&MaxSalePrice=&MinRentPrice=&MaxRentPrice=&MinBedrooms=&MaxBedrooms=&PropertyType=Flats+%2F+Apartments&HideLetOrSold=on", "property_type": "apartment"},
	        {"url": "https://www.iamtheagent.com/property-for-rent/search/?SearchType=rentals&Location=uk&Distance=40&MinSalePrice=&MaxSalePrice=&MinRentPrice=&MaxRentPrice=&MinBedrooms=&MaxBedrooms=&PropertyType=Houses&HideLetOrSold=on", "property_type": "house"},
            {"url": "https://www.iamtheagent.com/property-for-rent/search/?SearchType=rentals&Location=uk&Distance=40&MinSalePrice=&MaxSalePrice=&MinRentPrice=&MaxRentPrice=&MinBedrooms=&MaxBedrooms=&PropertyType=Bungalows&HideLetOrSold=on", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
       
        for item in response.xpath("//div[@id='list-view']/div[contains(@class,'search-result-property')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        studio = "".join(response.xpath("//div[@class='col-md-8']/h2/text()").extract())
        if studio in studio.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "normalize-space(//section[@id='property-details-title']//h2/text())")

        item_loader.add_value("external_source", "Iamtheagent_PySpider_united_kingdom")

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h4/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if ',' in address: item_loader.add_value("city", address.split(',')[-1].strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Full Description')]/following-sibling::text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//section[@id='property-details-title']//h2/text()").get()
        if room_count:
            try:
                item_loader.add_value("room_count", w2n.word_to_num(room_count.lower().split('bedroom')[0].strip()))
            except:
                pass
        
        bathroom_count = response.xpath("//text()[contains(.,'Bathroom') or contains(.,'bathroom')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", '1')

        rent = response.xpath("//h1/text()").get()
        if rent:
            if 'pcm' in rent.lower():
                rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')
            elif 'pw' in rent.lower():
                rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')

        import dateparser
        available_date = response.xpath("//h3[contains(.,'Available')]/following-sibling::text()[1][not(contains(.,'Now'))]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed: item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        deposit = response.xpath("//text()[contains(.,'Deposit') or contains(.,'deposit')]").get()
        if deposit:
            if '£' in deposit.lower().split('deposit')[-1]:
                item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.lower().split('deposit')[-1])))
            if 'month' in deposit.lower().split('deposit')[-1]:
                multiple = "".join(filter(str.isnumeric, deposit.lower().split('deposit')[-1]))
                if multiple: item_loader.add_value("deposit", str(int(multiple) * int(rent)))

        utilities = response.xpath("//ul[@class='list-unstyled item-list']/li/text()[contains(.,'month ')]").get()
        if utilities:
            uti = utilities.strip().split(" ")[0].strip().replace("+£","")
            item_loader.add_value("utilities", uti.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//section[@id='property-image-gallery']//ul[@class='pgwSlideshow']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'var latitude')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('var latitude =')[1].split(';')[0].strip())
            item_loader.add_value("longitude", latitude.split('var longitude =')[1].split(';')[0].strip())
        
        energy_label = response.xpath("//text()[contains(.,'EPC')]").get()
        if energy_label:
            if energy_label.lower().split('is')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.lower().split('is')[-1].strip().upper())

        floor = response.xpath("//br/following-sibling::text()[contains(.,'Floor')]").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.strip()))
            if floor: item_loader.add_value("floor", floor)

        pets_allowed = response.xpath("//text()[contains(.,'NO PETS') or contains(.,'No pets')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//h3[contains(.,'Furnishings')]/following-sibling::text()[1]").get()
        if furnished:
            if furnished.strip().lower() == 'furnished':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'unfurnished':
                item_loader.add_value("furnished", False)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "I AM THE AGENT")
        item_loader.add_value("landlord_email", "info@iamtheagent.com")

        yield item_loader.load_item()