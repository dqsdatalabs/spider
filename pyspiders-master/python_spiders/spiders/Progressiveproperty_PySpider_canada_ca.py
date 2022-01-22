import requests
from scrapy import Spider, Request
import scrapy
from python_spiders.loaders import ListingLoader

counter = 2

prob = ''
pos = 1


class Progressiveproperty_PySpider_ca_en(scrapy.Spider):
    name = 'Progressiveproperty_ca'
    allowed_domains = ['progressiveproperty.ca']
    country = 'canada'
    locale = 'en'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):

        start_urls = [

            {
                'url': 'https://progressiveproperty.ca/Real-Estate/Search?pmpm={%22368%22:{%22searchParams%22:{%22Keyword%22:%22%22,%22City%22:%220%22,%22propertyType%22:%227%22,%22minPrice%22:0,%22maxPrice%22:3000,%22bedroom%22:[%220%22,%221%22,%222%22,%223%22,%224%22]}}}',
                'property_type': 'apartment'},
            {
                'url': 'https://progressiveproperty.ca/Real-Estate/Search?pmpm={%22368%22:{%22searchParams%22:{%22Keyword%22:%22%22,%22City%22:%220%22,%22propertyType%22:%224%22,%22minPrice%22:0,%22maxPrice%22:3000,%22bedroom%22:[%220%22,%221%22,%222%22,%223%22,%224%22]}}}',
                'property_type': 'house'},
            {
                'url': 'https://progressiveproperty.ca/Real-Estate/Search?pmpm={%22368%22:{%22searchParams%22:{%22Keyword%22:%22%22,%22City%22:%220%22,%22propertyType%22:%225%22,%22minPrice%22:0,%22maxPrice%22:3000,%22bedroom%22:[%220%22,%221%22,%222%22,%223%22,%224%22]}}}',
                'property_type': 'house'},
            {
                'url': 'https://progressiveproperty.ca/Real-Estate/Search?pmpm={%22368%22:{%22searchParams%22:{%22Keyword%22:%22%22,%22City%22:%220%22,%22propertyType%22:%225%22,%22minPrice%22:0,%22maxPrice%22:3000,%22bedroom%22:[%220%22,%221%22,%222%22,%223%22,%224%22]}}}',
                'property_type': 'apartment'},

        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        global counter

        area_urls = response.xpath('.//a[@class="vpd vpd- sb"]/@href').extract()
        for area_url in area_urls:
            area_url = "https://progressiveproperty.ca/Real-Estate/" + area_url
            yield Request(url=area_url, callback=self.parse_area,
                          meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath(f'.//div[@class="mfp"]//a[@id="pgc-property-{counter}"]//@href').extract()
        if next_page:
            next_page = response.xpath(f'.//div[@class="mfp"]//a[@id="pgc-property-{counter}"]//@href').extract()[0]
            next_page = response.urljoin(next_page)
            counter += 1
            yield Request(url=next_page, callback=self.parse,
                          meta={'property_type': response.meta.get('property_type')})

        #

    def parse_area(self, response):
        global prob
        global pos
        ad = response.xpath(".//span[@class='propertyName left']//text()").extract()
        if ad[0] == 'PRE-APPLY':
            return
        else:
            prob = response.meta.get('property_type')
            item_loader = ListingLoader(response=response)
            # item_loader.add_value('property_type', )
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('landlord_name', 'Progressive Property')
            item_loader.add_value('landlord_phone', '(306)652-3322')
            ren = response.xpath(".//div[@class='price right']//text()").extract()[0]
            rent = int(ren[ren.find('$') + 1:ren.find('/')])
            item_loader.add_value('rent', int(rent))
            item_loader.add_value('currency', "CAD")
            item_loader.add_value('city',"Saskatoon")

            # item_loader.add_xpath('rent', ".//div[@class='price right']//text()")
            desc = response.xpath('.//div[contains(@class,"tab-content")]//text()').extract()[3].strip()
            print(desc)
            if len(desc)>3:
                item_loader.add_value('description', desc)
            else :
                desc = response.xpath('.//div[contains(@class,"tab-content")]//text()').extract()[1].strip()
                item_loader.add_value('description', desc)
            de = response.xpath( "/html/body/div/main/div/div/div/div/div[2]/div[1]/table[2]/tbody/tr[1]/td/div/p/text()").extract()
            for i in range(len(de)):
                if "parking" or "Parking" in de[i]:
                    item_loader.add_value('parking', True)
                if "laundry" in de[i]:
                    item_loader.add_value('washing_machine', True)
                if "dishwasher" in de[i]:
                    item_loader.add_value('dishwasher', True)
                if "balcony" or "Balcony" in de[i]:
                    item_loader.add_value('balcony', True)
                if "Pet friendly" or "PET FRIENDLY" in de[i]:
                    item_loader.add_value('pets_allowed', True)
            item_loader.add_value('property_type', prob)
            address=response.xpath(".//div[@class='propertyName']//text()").extract()
            item_loader.add_xpath('title', ".//span[@class='cityProv left']//text()")
            try :
                responseGeocode = requests.get( f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address},Saskatoon&maxLocations=1")
                responseGeocodeData = responseGeocode.json()
                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']
                longitude = str(longitude)
                latitude = str(latitude)
                item_loader.add_value('address', address)
                item_loader.add_value('zipcode', zipcode)
                item_loader.add_value('longitude', longitude)
                item_loader.add_value('latitude', latitude)
            except :
                pass
            try:
                room = int(response.xpath('.//div[contains(@class,"bedroom")]//text()').extract()[0])
            except:
                room=1
            bath = int(response.xpath('.//div[contains(@class,"bathroom")]//text()').extract()[0])
            item_loader.add_value('room_count', room)
            item_loader.add_value('bathroom_count', bath)
            imgs = response.xpath(".//a[@class='fancyBoxLink']//@href").extract()
            item_loader.add_value('images', imgs)
            item_loader.add_value('external_images_count', len(imgs))
            item_loader.add_value('position', pos)
            pos += 1

            yield item_loader.load_item()
