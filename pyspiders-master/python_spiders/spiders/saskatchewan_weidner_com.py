import scrapy
from ..loaders import ListingLoader
from ..helper import *

class WeidnerComSpider(scrapy.Spider):
    name = 'saskatchewan_weidner_com'
    start_urls = [ 'https://saskatchewan.weidner.com/searchlisting.aspx']
    allowed_domains = ['saskatchewan.weidner.com']
    country = 'canada'
    locale = 'en'
    external_source = '{}_PySpider_{}_{}'.format(name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    def start_requests(self):
        url = self.start_urls[0]
        yield scrapy.Request(url, callback=(self.parse), dont_filter=True)

    def parse(self, response, **kwargs):
        props_url = response.css('.propertyUrl::attr(href)').extract()
        for prop in props_url:
            yield scrapy.Request(prop, callback=(self.pages), dont_filter=True)

    def pages(self, response, **kwargs):
        loc = ' '.join(response.css('#address span span::text').extract()[1:4])
        yield scrapy.Request((response.url.split('index')[0] + 'floorplans'), meta={'location': loc}, callback=(self.inner_page), dont_filter=True)

    def inner_page(self, response):
        if response.css("script:contains('var pageData')").get() is not None:
            for i in [i.split(',')[0].replace('"', '').replace(' ', '-') for i in response.css("script:contains('var pageData')").get().split('pageData = ')[1].split(';</script>')[0].split('urlname: ')[1:]]:
                url = response.url + '/' + i
                yield scrapy.Request(url, meta={'location': response.meta['location']}, callback=(self.populate_item), dont_filter=True)

    def populate_item(self, response):
        longitude, latitude = extract_location_from_address(response.meta['location'])
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = ['https://cdngeneral.rentcafe.com/' + i for i in response.css('img::attr(src)').extract() if 'https:' not in i]
        room_count = 1
        if response.css('.single-fp-type').get() is not None:
            room_count = int(float(response.css('.single-fp-type').get().split('Bedroom')[0][-4:-1].strip()))
        bathroom_count = None
        if response.css('.single-fp-baths').get() is not None:
            bathroom_count = int(float(response.css('.single-fp-baths').get().split('Bath')[0][-4:-1].strip()))
        square_feet = None
        if response.css('.single-fp-sqft').get() is not None:
            square_feet = int(float(response.css('.single-fp-sqft').get().split('Sq.Ft')[0][-5:-1].strip()))
        title = response.css('#fp-name::text').get()
        description = ' '.join([i for i in response.css('#fp-leftSidebar li::text').extract() if i.strip() != ''])
        description = re.sub("[_,.*+(){}';@#?!&$/-]+\\ *", ' ', description)
        description = re.sub('[\\n\\r]', ' ', description)
        description = re.sub(' +', ' ', description)
        balcony = terrace = pets_allowed = swimming_pool = washing_machine = dishwasher = parking = elevator = None
        pets_names = ['pet friendly', 'pets friendly', 'pets allowed', 'pet allowed', 'cats friendly', 'cat friendly',
         'dogs fiendly', 'dog fiendly', 'cats allowed', 'cat allowed', 'dogs allowed', 'dog allowed']
        balcony_names = [
         'balcony', 'balconies']
        terraces = [
         'terrace', 'terraces']
        pool_names = [
         'pool', 'swimming', 'swimming pool']
        washing_names = [
         'washing machine', 'loundry', 'washer']
        dishwasher_names = [
         'dishwasher']
        park_names = [
         'parking', 'garage']
        elevator_names = [
         'elevator']
        if re.search('(?=(' + '|'.join(pets_names) + '))', description):
            pets_allowed = True
        if re.search('(?=(' + '|'.join(balcony_names) + '))', description):
            balcony = True
        if re.search('(?=(' + '|'.join(terraces) + '))', description):
            terrace = True
        if re.search('(?=(' + '|'.join(washing_names) + '))', description):
            washing_machine = True
        if re.search('(?=(' + '|'.join(dishwasher_names) + '))', description):
            dishwasher = True
        if re.search('(?=(' + '|'.join(park_names) + '))', description):
            parking = True
        if re.search('(?=(' + '|'.join(elevator_names) + '))', description):
            elevator = True
        if re.search('(?=(' + '|'.join(pool_names) + '))', description):
            swimming_pool = True
        landlord_name = 'texas weidner'
        landlord_number = '(479) 802-0445'
        landlord_email = 'texas weidner'
        ids = [re.search('>#(.*)</span>\\r\\n', i)[0].split('#')[1].split('<')[0] for i in response.css('.fp-availApt-Container').extract()]
        prices = [int(float(i.split('$')[1].split('<')[0].replace(',', ''))) for i in response.css('.fp-availApt-Container').extract()]

        if room_count == 0:
            room_count = 1

        for i in range(len(ids)):
            item_loader = ListingLoader(response=response)
            external_id = ids[i]
            rent = prices[i]
            item_loader.add_value('external_link', response.url+'#'+str(self.position))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('position', self.position)
            item_loader.add_value('title', title)
            item_loader.add_value('description', description)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('address', address)
            item_loader.add_value('latitude', str(latitude))
            item_loader.add_value('longitude', str(longitude))
            item_loader.add_value('property_type', 'apartment')
            item_loader.add_value('square_meters', square_feet)
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value('pets_allowed', pets_allowed)
            item_loader.add_value('parking', parking)
            item_loader.add_value('elevator', elevator)
            item_loader.add_value('balcony', balcony)
            item_loader.add_value('terrace', terrace)
            item_loader.add_value('swimming_pool', swimming_pool)
            item_loader.add_value('washing_machine', washing_machine)
            item_loader.add_value('dishwasher', dishwasher)
            item_loader.add_value('images', images[1:])
            item_loader.add_value('external_images_count', len(images))
            item_loader.add_value('floor_plan_images', images[0])
            item_loader.add_value('rent', rent)
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('landlord_name', landlord_name)
            item_loader.add_value('landlord_phone', landlord_number)
            item_loader.add_value('landlord_email', landlord_email)
            self.position += 1
            yield item_loader.load_item()
